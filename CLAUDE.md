# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Stata/Python pipeline that calculates morphine milligram equivalents (MME) from BC PharmaNet dispensing records held in the Population Data BC Secure Research Environment (SRE). All code runs inside the SRE except `prepare_hc_drug_files.py` (run externally to download Health Canada data).

## Running the pipeline

The entire pipeline is driven by a single Stata script:

```stata
do mme_master
```

Before running, edit the globals at the top of `mme_master.do`:

```stata
global dir          "R:\working\<you>\ppp"
global data_dir     "$dir\data"
global hc_dir       "$dir\hc"           /* where HC .txt files live */
global pnet_dir     "$dir\pnet"         /* where per-year CSVs go */
global data_version "R:\DATA\<yyyy-mm-dd>"
global start_year   2000
global end_year     2022
global min_age      16
global max_age      64
```

Log files are written alongside each script. The Python step (`strip_pnet_pq.py`) can also be tested standalone:

```bash
python strip_pnet_pq.py --input_dir "R:/DATA/.../PharmaNet/parquet/PharmaNet" --output_dir "R:/working/.../pnet" --start_year 2000 --end_year 2022 --min_age 16 --max_age 64
```

## Pipeline architecture

The scripts execute in the order shown in `mme_master.do`:

1. **`strip_pnet_pq.py`** — Python; reads PharmaNet parquet files (partitioned as `SRV_DATE_YEAR=*/SRV_DATE_MONTH=*/*.parquet`), applies age filter, writes one CSV per year to `$pnet_dir`. Called via Stata's `python script` command.

2. **`import_hc.do`** — Imports the five Health Canada DPD text files (`drug`, `ingred`, `route`, `form`, `ther`) into Stata `.dta` format in `$hc_dir`.

3. **`merge_hc_dip.do`** — Merges Health Canada DPD segments with the DIP drug list (`dip_drugs.dta`). Produces `hc_drugs.dta` and `dip_hc.dta`. DIP-only drugs (compounded products not in the HC DPD, representing ~40% of MME) use the DIP `gen_drug` field as fallback ingredient.

4. **`import_moh.do`** — Imports `mme.csv` (MoH opioid list with MME conversion factors per opioid/route).

5. **`opioid_names.do`** — Identifies opioids in the merged drug file through five phases: (1) name search in ingredient/brand fields, (2) manual brand-name parsing from `parse_brand_name.csv`, (3) manual strength parsing from `parse_strength.csv`, (4) route-of-administration assignment, (5) per-unit strength factor calculation and merge with MME conversion factors. Produces `dip_hc_opioids.dta`.

6. **`strip_opioids.do`** — Filters the per-year PharmaNet CSVs to opioid prescriptions only, using `dip_hc_opioids.dta` as the drug reference.

7. **`cnt_rx_2000_2021.do`** — Appends all per-year opioid files into `tot_opioid.dta`, computes empirical quantity distributions (median, p90 of `dspd_days_sply` and `dspd_qty` by `din_pin`), and merges with the opioid drug reference to produce `dip_med_mme.dta`.

## Key data files

| File | Description |
|------|-------------|
| `mme.csv` | MME conversion factors: `opioid_name × mme_route × strength_unit → cf` |
| `list_of_opioids.txt` | Master opioid name list used for string matching |
| `parse_brand_name.csv` | Manual lookup for DIP-only drugs where `ingredient = "Unknown Generic Drug"` |
| `parse_strength.csv` | Manual lookup for records still missing numeric strength after brand-name parsing |

## MME calculation

For each prescription: `MME = factor × cf × dspd_qty`

Where `factor` = per-unit strength (mg of active opioid per dispensed unit), and `cf` = MoH conversion factor from `mme.csv`. Route affects both `cf` selection and interpretation of `dspd_qty`. Patches use release rate (mcg/hr); oral and volume-based injectables divide strength by dosage volume.

## Prerequisites (inside SRE)

- Stata 16+ (required for `python script` command)
- Python 3.8+ with `pandas` and `pyarrow`

## Data sources (inside SRE)

- PharmaNet parquet: `R:\DATA\<date>\Ministry_of_Health\PharmaNet\parquet\PharmaNet`
- DIP drug list: `R:\DATA\<date>\Ministry_of_Health\PharmaNet\csv\pharmanet-hlth-prod_*.csv`
- Health Canada DPD: downloaded externally via `prepare_hc_drug_files.py`, imported via OCWA
