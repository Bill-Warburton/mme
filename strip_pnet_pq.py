"""
strip_pnet_pq.py
================
Extract selected fields from the BC PharmaNet parquet files and save one
CSV per year to the output directory.

This script is called by mme_master.do via Stata's -python script- command,
which passes arguments through Stata globals.  It can also be run directly
from the command line for testing purposes.

Usage (command line)
--------------------
python strip_pnet_pq.py \\
    --input_dir  "R:/DATA/2026-01-29/Ministry_of_Health/PharmaNet/parquet/PharmaNet" \\
    --output_dir "R:/working/Bill/ppp/pnet" \\
    --start_year 2000 \\
    --end_year   2022 \\
    --min_age    16   \\
    --max_age    64   \\
    --workers    4

Usage (called from Stata via -python script-)
---------------------------------------------
Stata passes arguments as a space-separated string after the script name.
mme_master.do sets a Stata global `strip_pnet_args` and calls:

    python script strip_pnet_pq.py, args("`strip_pnet_args'")

See mme_master.do for the exact invocation.

Outputs
-------
One CSV file per calendar year written to <output_dir>/<year>.csv.
Columns: studyid, DSPD_QTY, DSPD_DAYS_SPLY, SRV_DATE, DIN_PIN,
         PRSCR_PRAC_LIC_BODY_IDNT, PRSCR_PRAC_IDNT, CLNT_AGE_IN_YRS_NUM

Notes
-----
* Requires: pandas, pyarrow
* PharmaNet parquet data are partitioned as
      <input_dir>/SRV_DATE_YEAR=<yyyy>/SRV_DATE_MONTH=<mm>/*.parquet
* Only rows where CLNT_AGE_IN_YRS_NUM is in [min_age, max_age] are kept.
* CLNT_KEY is renamed to studyid on output to match downstream Stata code.
* Memory is released after each year to cope with the large file sizes.
* Parallel processing (--workers > 1) processes multiple years concurrently.
  Note: --workers > 1 may not work when called from Stata's embedded Python
  due to process-spawning constraints; use the default (1) in that context.
  Peak memory scales with the number of workers × per-year memory footprint.

Copyright 2024 Province of British Columbia
Licensed under the Apache License, Version 2.0
"""

import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIELDS_TO_EXTRACT = [
    "CLNT_KEY",
    "DSPD_QTY",
    "DSPD_DAYS_SPLY",
    "SRV_DATE",
    "DIN_PIN",
    "PRSCR_PRAC_LIC_BODY_IDNT",
    "PRSCR_PRAC_IDNT",
    "CLNT_AGE_IN_YRS_NUM",
]


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    """Parse command-line arguments (or the list supplied by Stata)."""
    parser = argparse.ArgumentParser(
        description="Strip selected fields from PharmaNet parquet files."
    )
    parser.add_argument(
        "--input_dir",
        required=True,
        help=(
            "Root directory of the partitioned PharmaNet parquet data, e.g. "
            "R:/DATA/2026-01-29/Ministry_of_Health/PharmaNet/parquet/PharmaNet"
        ),
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory where per-year CSV files will be written.",
    )
    parser.add_argument(
        "--start_year",
        type=int,
        default=2000,
        help="First calendar year to process (inclusive). Default: 2000.",
    )
    parser.add_argument(
        "--end_year",
        type=int,
        default=2022,
        help="Last calendar year to process (inclusive). Default: 2022.",
    )
    parser.add_argument(
        "--min_age",
        type=int,
        default=16,
        help="Minimum patient age to retain (inclusive). Default: 16.",
    )
    parser.add_argument(
        "--max_age",
        type=int,
        default=64,
        help="Maximum patient age to retain (inclusive). Default: 64.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "Number of parallel worker processes. Default: 1 (sequential). "
            "Set to a value greater than 1 to process multiple years "
            "concurrently using ProcessPoolExecutor. Values above the number "
            "of available CPU cores are unlikely to improve performance. "
            "Note: values > 1 may not work when called from Stata's embedded "
            "Python; use the default (1) in that context."
        ),
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_year(year_dir: Path, fields: list, min_age: int, max_age: int) -> pd.DataFrame:
    """
    Read all parquet files in a single year partition, apply the age filter,
    and return a concatenated DataFrame.

    Parameters
    ----------
    year_dir : Path
        Directory for one SRV_DATE_YEAR partition.
    fields : list of str
        Column names to extract.
    min_age, max_age : int
        Inclusive age bounds for filtering on CLNT_AGE_IN_YRS_NUM.

    Returns
    -------
    pd.DataFrame or None
        Filtered data for the year, or None if no parquet files were found.
    """
    monthly_frames = []

    for month_dir in sorted(year_dir.glob("SRV_DATE_MONTH=*")):
        if not month_dir.is_dir():
            continue
        month_str = month_dir.name.split("=")[1]
        print(f"  Processing month {month_str}...")

        for parquet_file in sorted(month_dir.glob("*.parquet")):
            df = pq.read_table(parquet_file, columns=fields).to_pandas()

            # Coerce age to numeric; non-numeric values become NaN and are dropped
            df["CLNT_AGE_IN_YRS_NUM"] = pd.to_numeric(
                df["CLNT_AGE_IN_YRS_NUM"], errors="coerce"
            )
            df = df[
                (df["CLNT_AGE_IN_YRS_NUM"] >= min_age)
                & (df["CLNT_AGE_IN_YRS_NUM"] <= max_age)
            ]
            monthly_frames.append(df)

    if not monthly_frames:
        return None

    return pd.concat(monthly_frames, ignore_index=True)


def process_and_save_year(
    year_int: int,
    year_dir_str: str,
    output_dir_str: str,
    fields: list,
    min_age: int,
    max_age: int,
) -> tuple:
    """
    Worker function: read, filter, and write one year's data to CSV.

    Writing happens inside the worker to avoid transferring large DataFrames
    back to the main process via IPC.  Arguments use plain strings rather than
    Path objects for reliable cross-process pickling on Windows.

    Returns
    -------
    tuple of (year_int, record_count_or_None, output_path_or_message)
    """
    year_df = process_year(Path(year_dir_str), fields, min_age, max_age)

    if year_df is None:
        return (year_int, None, "no data found")

    year_df.rename(columns={"CLNT_KEY": "studyid"}, inplace=True)
    output_file = Path(output_dir_str) / f"{year_int}.csv"
    year_df.to_csv(output_file, index=False)
    return (year_int, len(year_df), str(output_file))


# ---------------------------------------------------------------------------
# Main run logic
# ---------------------------------------------------------------------------

def run(input_dir: str, output_dir: str,
        start_year: int, end_year: int,
        min_age: int, max_age: int,
        workers: int = 1) -> None:
    """
    Main processing loop: iterate over year partitions and write CSV outputs.

    Parameters
    ----------
    input_dir : str
        Root of the PharmaNet parquet partition tree.
    output_dir : str
        Destination directory for per-year CSV files.
    start_year, end_year : int
        Inclusive year range to process.
    min_age, max_age : int
        Inclusive patient age range to retain.
    workers : int
        Number of parallel worker processes.  1 = sequential (default).
    """
    input_base = Path(input_dir)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_base.exists():
        raise FileNotFoundError(f"Input directory not found: {input_base}")

    ts = lambda: pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # Collect year directories within the requested range
    year_tasks = []
    for year_dir in sorted(input_base.glob("SRV_DATE_YEAR=*")):
        if not year_dir.is_dir():
            continue
        try:
            year_int = int(year_dir.name.split("=")[1])
        except (ValueError, IndexError):
            print(f"Skipping unrecognised directory: {year_dir.name}")
            continue
        if year_int < start_year or year_int > end_year:
            continue
        year_tasks.append((year_int, year_dir))

    if not year_tasks:
        print("No year directories found in the requested range.")
        return

    # ------------------------------------------------------------------
    # Sequential path (workers == 1)
    # ------------------------------------------------------------------
    if workers == 1:
        for year_int, year_dir in year_tasks:
            print(f"Processing year {year_int} at {ts()}...")
            year_df = process_year(year_dir, FIELDS_TO_EXTRACT, min_age, max_age)

            if year_df is None:
                print(f"  No data found for year {year_int}.")
                continue

            year_df.rename(columns={"CLNT_KEY": "studyid"}, inplace=True)
            output_file = out_dir / f"{year_int}.csv"
            year_df.to_csv(output_file, index=False)
            print(f"  Saved {len(year_df):,} records to {output_file}")
            print(f"  Completed at {ts()}")
            del year_df

    # ------------------------------------------------------------------
    # Parallel path (workers > 1)
    # ------------------------------------------------------------------
    else:
        n_workers = min(workers, len(year_tasks))
        print(
            f"Processing {len(year_tasks)} years using {n_workers} workers "
            f"at {ts()}..."
        )

        future_to_year = {}
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            for year_int, year_dir in year_tasks:
                future = executor.submit(
                    process_and_save_year,
                    year_int,
                    str(year_dir),
                    str(out_dir),
                    FIELDS_TO_EXTRACT,
                    min_age,
                    max_age,
                )
                future_to_year[future] = year_int

            for future in as_completed(future_to_year):
                year_int = future_to_year[future]
                try:
                    yr, n, detail = future.result()
                    if n is None:
                        print(f"  Year {yr}: no data found.")
                    else:
                        print(f"  Year {yr}: saved {n:,} records to {detail} at {ts()}")
                except Exception as exc:
                    print(f"  Year {year_int}: FAILED — {exc}")

    print(f"Processing complete at {ts()}!")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    run(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        start_year=args.start_year,
        end_year=args.end_year,
        min_age=args.min_age,
        max_age=args.max_age,
        workers=args.workers,
    )
