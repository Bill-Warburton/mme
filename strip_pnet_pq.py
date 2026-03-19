import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

# Configuration
input_base_dir = Path("""R:/DATA/2026-01-29/Ministry_of_Health/PharmaNet/parquet/PharmaNet""")  
output_dir = Path("""R:/working/Bill/ppp/pnet""")
output_dir.mkdir(parents=True, exist_ok=True)

# Fields to extract
fields_to_extract = ['CLNT_KEY', 'DSPD_QTY', 'DSPD_DAYS_SPLY', 'SRV_DATE', 'DIN_PIN', 'PRSCR_PRAC_LIC_BODY_IDNT', 'PRSCR_PRAC_IDNT', 'CLNT_AGE_IN_YRS_NUM']  # Add your fields here

# Year range
start_year = 2000
end_year = 2022

# Process each year
for year_dir in sorted(input_base_dir.glob("SRV_DATE_YEAR=*")):
    if not year_dir.is_dir():
        continue
    
    year_str = year_dir.name.split('=')[1]
    print(f"Processing year {year_str} at {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}...")
 # Skip years outside the range
    try:
        year_int = int(year_str)
        if year_int < start_year or year_int > end_year:
            continue
    except (ValueError, IndexError):
        # Skip if directory name is not a valid year
        continue
    
    year_data = []
    
    print(f"Processing year {year_str}...")
    
    # Process each month in the year
    for month_dir in sorted(year_dir.glob("SRV_DATE_MONTH=*")):
        if not month_dir.is_dir():
            continue
        month_str = month_dir.name.split('=')[1]
        print(f"  Processing {year_str}/{month_str}...")
        
        # Process all parquet files in the month directory
        for parquet_file in month_dir.glob("*.parquet"):
            # Read only selected columns with filtering
            df = pq.read_table(
                parquet_file,
                columns=fields_to_extract
            ).to_pandas()
            df['CLNT_AGE_IN_YRS_NUM'] = pd.to_numeric(df['CLNT_AGE_IN_YRS_NUM'],errors='coerce')
            df = df[df['CLNT_AGE_IN_YRS_NUM'] >=16 & (df['CLNT_AGE_IN_YRS_NUM'] <= 64)]
            year_data.append(df)
    
    # Combine all months for this year
    if year_data:
        year_df = pd.concat(year_data, ignore_index=True)
        
        # Rename CLNT_KEY to studyid
        year_df.rename(columns={'CLNT_KEY': 'studyid'}, inplace=True)
        
        # Save to CSV
        output_file = output_dir / f"{year_str}.csv"
        year_df.to_csv(output_file, index=False)
        print(f"Saved {len(year_df)} records to {output_file}")
        print(f"Completed year {year_str} at {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Clear memory
        del year_df
        del year_data
    else:
        print(f"No data found for year {year}")

print("Processing complete!")