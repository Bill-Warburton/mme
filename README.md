# mme — Morphine Milligram Equivalent Calculator for BC PharmaNet

Stata/Python code for calculating milligrams of morphine equivalent (MME) from BC PharmaNet dispensing records held in the Population Data BC Secure Research Environment (SRE).

---

## Table of Contents

1. [Background](#background)
2. [Repository structure](#repository-structure)
3. [Data sources](#data-sources)
4. [Prerequisites](#prerequisites)
5. [Quick start](#quick-start)
6. [Step-by-step setup](#step-by-step-setup)
   - [1. Download and prepare Health Canada drug files](#1-download-and-prepare-health-canada-drug-files)
   - [2. Import Health Canada files into the SRE via OCWA](#2-import-health-canada-files-into-the-sre-via-ocwa)
   - [3. Import the project code into the SRE via OCWA](#3-import-the-project-code-into-the-sre-via-ocwa)
   - [4. Configure mme_master.do](#4-configure-mme_mastero)
   - [5. Run the pipeline](#5-run-the-pipeline)
7. [Pipeline description](#pipeline-description)
8. [Opioid identification](#opioid-identification)
9. [MME conversion factors](#mme-conversion-factors)
10. [Known data issues](#known-data-issues)
11. [Suggested improvements](#suggested-improvements)
12. [References](#references)
13. [Researchers who have used PharmaNet data](#researchers-who-have-used-pharmanet-data)
14. [License](#license)

---

## Background

Morphine milligram equivalents (MME) provide a standardised way to compare the potency of different opioid prescriptions.  This project calculates MME for all opioid prescriptions dispensed in British Columbia and recorded in the PharmaNet administrative database.

The main entry point is `mme_master.do`.  Running it executes the complete pipeline and produces aggregate MME statistics by year.

---

## Repository structure

```
mme/
├── mme_master.do           Master script — run this to execute the pipeline
├── strip_pnet_pq.py        Python: extract fields from PharmaNet parquet files
├── prepare_hc_drug_files.py Python: download & consolidate Health Canada files
├── import_hc.do            Import Health Canada text files into Stata
├── import_moh.do           Import MoH opioid list and conversion factors
├── merge_hc_dip.do         Merge Health Canada and DIP drug lists
├── opioid_names.do         Identify opioids; attach conversion factors
├── strip_opioids.do        Filter PharmaNet CSVs to opioid prescriptions only
├── cnt_rx_2000_2021.do     Aggregate prescriptions and calculate MME
├── mme.csv                 Conversion factors (opioid × route)
├── list_of_opioids.txt     Master opioid name list
├── parse_brand_name.csv    Manual parsing of brand-name fields for DIP records
└── README.md               This file
```

---

## Data sources

| Source | Description | How obtained |
|--------|-------------|--------------|
| **PharmaNet parquet files** | Dispensing records for all BC prescriptions | Available inside the SRE at `R:\DATA\<date>\Ministry_of_Health\PharmaNet\parquet\PharmaNet` |
| **DIP drug list (hlth_prod)** | List of all drugs with a PharmaNet record | Available inside the SRE at `R:\DATA\<date>\Ministry_of_Health\PharmaNet\csv\pharmanet-hlth-prod_*.csv` |
| **Health Canada Drug Product Database (DPD)** | Drug ingredients, strengths, forms, routes, ATC codes | Downloaded externally using `prepare_hc_drug_files.py`; imported via OCWA |
| **MoH MME conversion factors by din_pin** | OME conversion factors provided by BC Ministry of Health in Appendix 1 of the June 2025 edition of BC Prescription Drug Atlas
Opioids and Benzodiazepine Receptor Agonists available at https://www2.gov.bc.ca/assets/gov/health/health-drug-coverage/pharmacare/drug-data/bc_prescription_drug_atlas_2025_-_opioids_and_bzras.pdf

### Note on drugs not in the Health Canada DPD

Approximately 40% of MME in the 2000–2020 period is attributable to drugs that appear in the PharmaNet DIP list but are absent from the Health Canada DPD.  These drugs are believed to be products compounded by BC pharmacies, which are not required to register with Health Canada.  For a discussion of compounded medications in administrative data, see Dormuth et al. (2019) referenced below.  Where the DIP list `strength` field is populated, it contains both the strength value and its units (and, for multi-ingredient products, the strengths of all ingredients combined).  Where only `drug_brand_nm` is filled, manual parsing results are stored in `parse_brand_name.csv`.

---

## Prerequisites

**Inside the SRE**

- Stata 16 or later (required for the `python script` command)
- Python 3.8 or later with:
  - `pandas`
  - `pyarrow`

**Outside the SRE (for Health Canada file preparation)**

- Python 3.8 or later with:
  - `requests` (`pip install requests`)

---

## Quick start

If you have already imported the Health Canada files and project code into the SRE:

1. Open `mme_master.do` in Stata.
2. Edit the six globals at the top of the file (paths and year/age ranges).
3. Run the script: `do mme_master`.

---

## Step-by-step setup

### 1. Download and prepare Health Canada drug files

Run `prepare_hc_drug_files.py` **outside** the SRE (on your local machine or any internet-connected computer):

```bash
pip install requests
python prepare_hc_drug_files.py --output_dir C:\temp\hc_drug_files
```

This script downloads all approved, marketed, cancelled, and dormant product records from the [Health Canada Drug Product Database](https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/what-data-extract-drug-product-database.html) for the five segments used by this project (`drug`, `ingred`, `route`, `form`, `ther`), extracts the zip archives, and writes one consolidated `.txt` file per segment.

> **Why .txt?**  OCWA's automated scanning rejects `.zip` files.  The script consolidates each segment into a single plain-text file that passes OCWA's import rules.

### 2. Import Health Canada files into the SRE via OCWA

1. Connect to the Popdata VPN ([instructions](https://my.popdata.bc.ca/html/SRE/windows/connecting.html)).
2. Navigate to [http://ocwadl.popdata.bc.ca/](http://ocwadl.popdata.bc.ca/) and log in with your **PopData project username** (e.g. `pripley-99-t04`; your "parent" account will not work).
3. Click **New Request** → Import Type: **Other Import (e.g., documentation, code lists)**.
4. Upload the five `.txt` files from Step 1.  Note that OCWA limits individual files to 20 MB and the total per request to 50 MB.  If the files exceed these limits, submit two separate import requests.
5. Click **Done Editing**, then **Submit Request**.
6. Once the automated scan passes and the request is approved, connect to an SAE workstation, open Chrome, navigate to [http://ocwa.popdata.bc.ca/](http://ocwa.popdata.bc.ca/), and click **Downloads** to retrieve the files.
7. Copy the `.txt` files into the directory set as `$hc_dir` in `mme_master.do`.

### 3. Import the project code into the SRE via OCWA

The project code is publicly available at [https://github.com/Bill-Warburton/mme](https://github.com/Bill-Warburton/mme).  To import via OCWA Code Import:

1. Connect to the Popdata VPN.
2. Navigate to [http://ocwadl.popdata.bc.ca/](http://ocwadl.popdata.bc.ca/) and log in.
3. Click **New Request** → Import Type: **Code Import**.
4. Grant PopData's GitHub account (`popdata-ocwa-user`) collaborator access to your fork of the repository (Settings → Collaborators), then wait for PopData to accept.  If your repository is public, this step can be skipped.
5. Fill in the internal and external repository URLs and the branch name, then click **Create Request** → **Done Editing**.
6. Once the merge request finishes, click **Submit Request**.
7. Access the approved code inside the SAE at [https://projectsc.popdata.bc.ca/](https://projectsc.popdata.bc.ca/).

### 4. Configure mme_master.do

Open `mme_master.do` and update the globals near the top:

```stata
global dir          "R:\working\<you>\ppp"          /* your working directory */
global data_dir     "$dir\data"
global hc_dir       "$dir\hc"                        /* where HC .txt files live */
global pnet_dir     "$dir\pnet"                      /* where per-year CSVs go */
global data_version "R:\DATA\<yyyy-mm-dd>"           /* your DIP data snapshot */
global start_year   2000
global end_year     2022
global min_age      16
global max_age      64
```

### 5. Run the pipeline

Inside Stata, with the working directory set:

```stata
do mme_master
```

Log files are written alongside the scripts.  The final output is produced by `cnt_rx_2000_2021.do`.

---

## Pipeline description

### `strip_pnet_pq.py`

Reads the PharmaNet parquet partition tree (`SRV_DATE_YEAR=*/SRV_DATE_MONTH=*/*.parquet`), extracts eight fields, applies an age filter, and writes one CSV per year.  Called from `mme_master.do` via Stata's `python script` command with all paths and the age range passed as command-line arguments.

Extracted fields:

| Field | Description |
|-------|-------------|
| `CLNT_KEY` (renamed `studyid`) | De-identified patient identifier |
| `DSPD_QTY` | Quantity dispensed |
| `DSPD_DAYS_SPLY` | Days supplied |
| `SRV_DATE` | Date of dispensing |
| `DIN_PIN` | Drug Identification Number or PIN |
| `PRSCR_PRAC_LIC_BODY_IDNT` | Prescriber licensing body |
| `PRSCR_PRAC_IDNT` | Prescriber identifier |
| `CLNT_AGE_IN_YRS_NUM` | Patient age in years |

### `import_hc.do`

Imports the five consolidated Health Canada text files into Stata `.dta` format.

### `merge_hc_dip.do`

Merges the Health Canada drug file with the DIP drug list (hlth_prod).  Because multi-ingredient drugs produce multiple rows per DIN/PIN in the Health Canada files, this step also collapses to one row per DIN/PIN for opioid-relevant fields.

### `import_moh.do`

Imports `mme.csv` — the MoH opioid list with one conversion factor per opioid/route combination.

### `opioid_names.do`

Identifies opioid records in the merged drug file by searching for opioid names in both the Health Canada ingredient fields and the DIP `drug_brand_nm` field.  Attaches the appropriate MME conversion factor.  Manual parsing results for brand-name-only records are read from `parse_brand_name.csv`.

### `strip_opioids.do`

Merges the opioid drug file with each year's PharmaNet CSV and retains only opioid prescriptions.

### `cnt_rx_2000_2021.do`

Collates opioid prescriptions across all years, calculates MME per prescription, and produces summary statistics including medians and 90th percentiles for `DSPD_DAYS_SPLY` and `DSPD_QTY`.

---

## Opioid identification

The opioid name list (`list_of_opioids.txt`) was built as follows:

1. Started with the opioid list from the [NIH HEAL initiative](https://github.com/heal).
2. Added opioids with ATC code N02A that were not on the HEAL list: **nalbuphine** and **propoxyphene** (HCl, NAP, and dextro- forms).  Nalbuphine has limited abuse potential and is not a controlled substance; propoxyphene was withdrawn from the Canadian market in 2010 due to cardiac arrhythmia risk.
3. Added **diamorphine** (heroin), which is absent from the HEAL list because heroin-assisted treatment operates under different regulatory frameworks in the US compared to Canada.

---

## MME conversion factors

> **Research and surveillance use only.** These factors are for epidemiological and population-level analysis. They must not be used to guide clinical opioid switching or dose conversion. All sources cited below include equivalent warnings.

Conversion factors in `mme.csv` are drawn primarily from the [BC Ministry of Health Prescription Drug Atlas — Opioids and Benzodiazepine Receptor Agonists (2025)](https://www2.gov.bc.ca/assets/gov/health/health-drug-coverage/pharmacare/drug-data/bc_prescription_drug_atlas_2025_-_opioids_and_bzras.pdf), Appendix 1 (drug list with OME per DIN/PIN) and Appendix 2 (conversion factor tables), and cross-checked against:

- [NIH HEAL MME conversion factors](https://github.com/heal) — Adams et al. (2025), *PAIN*
- [UCSF Pain Management Education — Oral Morphine Equivalent calculations](https://pain.ucsf.edu/opioid-analgesics/calculation-oral-morphine-equivalents-ome) — based on Nielsen et al. (2016)

The route codes used in `mme.csv` are: **1** oral or rectal · **2** transdermal patch · **3** injectable · **4** nasal or spray · **5** buccal or sublingual.

### Full conversion factor comparison

The table below lists every drug–route–unit combination in `mme.csv` alongside the corresponding value from each reference source. Cells marked **‡** differ from the `mme.csv` value; cells marked `—` indicate the drug–route combination is not listed in that source.

#### Buprenorphine

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Buprenorphine | Buccal/SL (5) | MCG | 0.040 | 0.030–0.040 | **‡ 0.03** (buccal film) | 0.030–0.040 | HEAL distinguishes SL (0.04) from buccal film (0.03). BC Atlas lists 0.03 for Belbuca film. CDC omits buprenorphine for pain MME. |
| Buprenorphine | Buccal/SL (5) | MG | 38.80 | **‡ 30–40** | not listed | **‡ 30–40** | Equivalent to 0.03–0.04/MCG × 1000. mme.csv value of 38.8 sits between buccal (30) and SL (40) published values. |
| Buprenorphine | Transdermal (2) | MCG | 2.200 | 2.4 | **‡ 12.6** | 2.4 | BC Atlas value is per-patch-per-day (incorporates patch rate × 7 days ÷ 7). HEAL and UCSF use 2.4/MCG/hr as a daily rate; both are equivalent when applied correctly. |
| Buprenorphine | Injectable (3) | MG | 38.80 | ~40 | not listed | not listed | Parenteral buprenorphine for pain; limited coverage in population-level atlases. |
| Buprenorphine | Oral (1) | MG | 38.80 | ~40 | not listed | not listed | Oral buprenorphine for pain (not OAT). Excluded from BC Atlas scope (OAT/PA drugs not included). |
| Buprenorphine | Oral (1) | MCG | 0.040 | 0.040 | not listed | 0.040 | Per-MCG equivalent of the oral/SL factor. |

#### Butorphanol

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Butorphanol | Injectable (3) | MG | 15.0 | not listed | not listed | 15.0 | Mixed agonist–antagonist; no injectable product in BC Atlas. |
| Butorphanol | Nasal/spray (4) | MG | 7.0 | not listed | 7.0 | 7.0 | BC Atlas confirms 7 OME/MG for 10 MG/ML nasal spray. HEAL does not list butorphanol. |

#### Codeine

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Codeine | Oral/rectal (1) | MG | 0.150 | 0.15 | 0.15 | 0.15 | Consistent across all sources. BC Atlas also lists injection at 0.25 (parenteral bioavailability correction). |

#### Dihydrocodeine

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Dihydrocodeine | Oral (1) | MG | 0.250 | 0.25 | not listed | 0.25 | Not marketed in BC; absent from BC Atlas. Consistent in HEAL and UCSF. |

#### Fentanyl

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Fentanyl | Buccal/SL (5) | MCG | 0.130 | 0.13 | 0.13 | 0.13 | Consistent across all sources for SL/buccal tablet and lozenge. |
| Fentanyl | Nasal/spray (4) | MCG | 0.160 | 0.16 | not listed | 0.16 | ~23% greater bioavailability than buccal lozenge. No nasal fentanyl product listed separately in BC Atlas. |
| Fentanyl | Oral film/spray (1) | MCG | 0.180 | 0.18 | 0.18 (buccal film) | 0.18 | Higher-bioavailability buccal film formulations. Consistent across sources. |
| Fentanyl | Injectable (3) | MG | 100.0 | 100 | **‡ 200** (IV) | 100 | BC Atlas lists IV fentanyl at 2/MCG for some products (see note 1). HEAL and UCSF use 100/MG consistently. |
| Fentanyl | Injectable (3) | MCG | 0.100 | 0.10 | **‡ 0.2** (inj) / **‡ 2** (IV) | 0.10 | BC Atlas lists injection at 0.2/MCG and IV at 2/MCG — likely reflecting specific product records or a bolus vs. infusion distinction. |
| Fentanyl | Transdermal (2) | MCG | 2.400 | 2.4 | **‡ 7.2** | 2.4 | BC Atlas 7.2 = 2.4 × 3 days (patch duration). HEAL and UCSF report 2.4/MCG/hr as the daily rate. Equivalent when applied correctly (see note 2). |

#### Hydrocodone

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Hydrocodone | Oral (1) | MG | 1.0 | 1.0 | 1.0 | 1.0 | Consistent across all sources. |

#### Hydromorphone

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Hydromorphone | Oral (1) | MG | 5.0 | 5.0 | 5.0 | 5.0 | mme.csv, HEAL, BC Atlas, and UCSF all use 5.0. Note: CDC/CMS uses 4.0 — a well-known discrepancy. The 5.0 follows Nielsen et al. (2016). |
| Hydromorphone | Oral (1) | MCG | 0.005 | 0.005 | 0.005 | 0.005 | Per-MCG equivalent of the 5.0/MG oral factor. |

#### Levorphanol

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Levorphanol | Oral (1) | MG | 11.0 | 11.0 | not listed | 11.0 | Not marketed in Canada; absent from BC Atlas. Consistent across HEAL and UCSF. |

#### Meperidine (Pethidine)

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Meperidine | Oral (1) | MG | 0.100 | 0.10 | 0.10 | 0.10 | Consistent across all sources. BC Atlas also lists injection at 0.4/MG. |

#### Methadone

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Methadone | Oral (1) | MG | 4.700 | **‡ 3–12** (dose-dep.) | **‡ 3.0** | **‡ 3–12** (variable) | All sources acknowledge non-linear dose-dependence. BC Atlas uses a single conservative value of 3.0 for population surveillance. HEAL recommends a 3–12 sliding scale. mme.csv = 4.7 is an intermediate epidemiological estimate. |

#### Morphine (reference drug)

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Morphine | Oral (1) | MG | 1.0 | 1.0 | 1.0 | 1.0 | Reference drug by definition. BC Atlas also lists IV/injection at 3.0. |

#### Nalbuphine

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Nalbuphine | Oral (1) | MG | 3.0 | not listed | 3.0 (injection) | ~3.0 | BC Atlas lists 3.0 for nalbuphine injection. Mixed agonist–antagonist; not in HEAL. Note: nalbuphine has no oral formulation; the route-1 code in mme.csv likely represents parenteral use. |

#### Opium

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Opium | Oral (1) | MG | 1.0 | not listed | 1.0 | 1.0 | Treated as equivalent to morphine content where listed. |

#### Oxycodone

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Oxycodone | Oral (1) | MG | 1.5 | 1.5 | 1.5 | 1.5 | Consistent across all sources. |

#### Oxymorphone

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Oxymorphone | Oral (1) | MG | 3.0 | 3.0 | 3.0 | 3.0 | Consistent for oral route. BC Atlas also lists rectal at 3.5 and injection at 30. |

#### Pentazocine

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Pentazocine | Oral (1) | MG | 0.370 | 0.37 | 0.37 | 0.37 | Consistent across all sources. Mixed agonist–antagonist. BC Atlas also lists injection at 1.0. |

#### Propoxyphene (withdrawn 2010)

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Propoxyphene HCl | Oral (1) | MG | 0.150 | not listed | 0.15 | 0.15 | Withdrawn from US and Canadian markets 2010. BC Atlas retains for historical data. |
| Propoxyphene Napsylate | Oral (1) | MG | 0.230 | not listed | not listed | ~0.23 | Napsylate salt has lower mg-for-mg potency than HCl salt (~65% by weight). |

#### Remifentanil

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Remifentanil | IV/research (listed as 1) | MG | 300.0 | not listed | 300 (IV) | not listed | Anaesthesia/procedural use only; no oral formulation exists. Route code 1 in mme.csv is likely a proxy for IV. |

#### Tapentadol

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Tapentadol | Oral (1) | MG | 0.300 | 0.30 | **‡ 0.4** | **‡ 0.4** | mme.csv and HEAL 2025 use 0.3 (revised down from 0.4 based on clinical evidence). BC Atlas and UCSF retain the older 0.4 value (= CDC/CMS). One of the most significant current inter-source discrepancies. |

#### Tramadol

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Tramadol | Oral (1) | MG | 0.200 | 0.20 | 0.2 | 0.20 | mme.csv, HEAL, BC Atlas, and UCSF all use 0.2. Note: CDC/CMS uses 0.1 — a well-known discrepancy. The 0.2 follows Nielsen et al. (2016). |

#### Diacetylmorphine / Diamorphine (Heroin)

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Diacetylmorphine | Oral (1) | MG | 2.500 | not listed | not listed | 2.5 | BC Atlas excludes OAT/PA drugs from Appendix 1 by design. Factor 2.5 is consistent with published literature where listed. Relevant for BC prescribed safer supply (PA) programs. |
| Diamorphine | Oral (1) | MG | 2.500 | not listed | not listed | 2.5 | Same drug as diacetylmorphine (INN name). Injectable diamorphine ≈ 3× oral potency. |

#### Dextropropoxyphene (withdrawn)

| Drug | Route (code) | Unit | mme.csv | NIH HEAL 2025 | BC Atlas 2024 App. 1 | UCSF OME | Notes |
|------|-------------|------|---------|---------------|----------------------|----------|-------|
| Dextropropoxyphene | Oral (1) | MG | 0.600 | not listed | not listed | **‡ ~0.23** | Withdrawn globally. UCSF and some Canadian sources cite ~0.23/MG (similar to propoxyphene napsylate). The 0.6 in mme.csv may reflect a different salt form or older reference. Factor varies substantially across historical sources. |

### Notes on specific discrepancies

1. **Fentanyl IV (BC Atlas):** BC Atlas Appendix 1 lists fentanyl injection at 0.2 OME/MCG and IV at 2 OME/MCG, both higher than the 0.1/MCG used in mme.csv, HEAL, and UCSF. These likely reflect a distinction between bolus injection and continuous IV infusion for specific registered products. Most other sources use 0.1/MCG (= 100/MG) for all parenteral fentanyl, consistent with a morphine:fentanyl IV potency ratio of 100:1.

2. **Fentanyl transdermal (BC Atlas vs. others):** The BC Atlas value of 7.2 is a per-patch-per-day figure that incorporates the 3-day patch duration (2.4 × 3 = 7.2). HEAL and UCSF report 2.4 OME per MCG/hr as a daily rate that must be multiplied separately by days worn. Both approaches yield the same daily OME when applied correctly. See also the `Patch day counting` suggested improvement in [Suggested improvements](#suggested-improvements).

3. **Hydromorphone (CDC vs. others):** The CDC/CMS value is 4.0, derived from the product monograph conversion. The 5.0 used in Canada and by UCSF follows Nielsen et al. (2016), which found a ratio closer to 1:5.

4. **Tramadol (CDC vs. HEAL/BC/UCSF):** The CDC/CMS value of 0.1 is derived from approved product labelling. The 0.2 used by Canadian authorities, HEAL, and UCSF reflects the Nielsen et al. (2016) systematic review synthesis.

5. **Tapentadol:** The 2025 NIH HEAL revision from 0.4 to 0.3 is based on emerging evidence that tapentadol's noradrenaline reuptake inhibition contributes meaningfully to analgesia and that the equianalgesic ratio to morphine is lower than originally estimated. BC Atlas and UCSF have not yet incorporated this revision.

6. **Methadone:** All reputable sources note that methadone's MME is highly dose-dependent due to its long and variable half-life and NMDA receptor activity. Fixed single values are approximations for surveillance only.

7. **Buprenorphine:** CDC does not include a pain-use MME factor for buprenorphine (it is treated as a partial agonist outside the overdose-risk framework). HEAL 2025, BC Atlas, and UCSF include factors for prescribed pain formulations (Butrans patch, Belbuca film).

### Choice of factors for this project

Where the BC Atlas (MoH) figure differs from other sources, the MoH value is used as the primary estimate because it reflects BC-specific practice guidelines and the PharmaNet product registry. The two most consequential divergences from CDC/CMS are the use of **0.2 for tramadol** (vs. CDC 0.1) and **5.0 for hydromorphone** (vs. CDC 4.0), both of which follow Nielsen et al. (2016) and align with BC Atlas.

### Interpreting dispensed quantity (`DSPD_QTY`)

Per the [PharmaCare Correct Quantities Policy](https://www2.gov.bc.ca/gov/content/health/practitioner-professional-resources/pharmacare/pharmacies/correct-quantities) and [PharmaCare Policy Manual section 5.5](https://www2.gov.bc.ca/gov/content/health/practitioner-professional-resources/pharmacare/pharmacare-publications/pharmacare-policy-manual-2012):

- **Tablets, oral liquids, patches**: quantity is unambiguous (number of tablets; volume in mL; number of patches).
- **Injectable liquids**: quantity may be volume in mL or number of vials.
- **Powders for injection**: quantity may be grams or number of vials.
- **Sprays**: number of doses or volume in mL.

For ambiguous injectable forms, the choice between using the Health Canada `strength` value versus the `strength/dosage value` ratio depends on whether quantity was entered as volume or vials.  In practice this determination is made per product; see the comments in `opioid_names.do`.

---

## Known data issues

**Diamorphine (DIN 02525003, June 2022):** Mean dispensed quantity is 4, days supplied is 1.  The product monograph for Diamorphine hydrochloride powder states a maximum dose of 1 g/day and recommends reconstituting to 100 mg/mL.  Using the listed Health Canada strength of 5,000 mg/vial and assuming 4 vials/day gives an implausibly high figure; using 100 mg/mL and the measured volume gives approximately 1,000 MME/day, which is high but consistent with supervised heroin-assisted treatment.  This record requires case-by-case review.

---

## Suggested improvements

The following enhancements are recommended for future versions:

1. **Sensitivity analysis options** — Allow `mme_master.do` to be run with alternative opioid lists (HEAL vs. MoH Atlas) and/or alternative conversion factor sets, controlled by a global flag.

2. **Outlier handling** — Add an option to replace `DSPD_QTY` values more than 10 times the 90th percentile with the within-drug median, to reduce the influence of data entry errors.

3. **Patch day counting** — Add an option to use a fixed number of days per patch (consistent with the Atlas methodology) rather than the `DSPD_DAYS_SPLY` variable, which may reflect the dispensing interval rather than the actual wearing period.

4. **Parallelisation** — `strip_pnet_pq.py` processes years sequentially.  For large date ranges, performance could be improved using `concurrent.futures.ProcessPoolExecutor`.

5. **Logging improvements** — Redirect Python `print` output to a file that mirrors the Stata log, so that the Step 1 Python run is fully captured in the audit trail.

6. **Automated Health Canada file updates** — Schedule or document the process for refreshing the Health Canada DPD files when new versions are published by Health Canada.

---

## References

- BC Ministry of Health. *BC Prescription Drug Atlas — Opioids and Benzodiazepine Receptor Agonists*, 2025. https://www2.gov.bc.ca/assets/gov/health/health-drug-coverage/pharmacare/drug-data/bc_prescription_drug_atlas_2025_-_opioids_and_bzras.pdf
- Health Canada. *Drug Product Database*. https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database.html
- NIH HEAL Initiative. *MME conversion factors and opioid name list*. https://github.com/heal
- UCSF Pain Management Education. *Calculation of Oral Morphine Equivalents (OME)*. https://pain.ucsf.edu/opioid-analgesics/calculation-oral-morphine-equivalents-ome
- BC PharmaCare. *Correct Quantities Policy*. https://www2.gov.bc.ca/gov/content/health/practitioner-professional-resources/pharmacare/pharmacies/correct-quantities
- BC PharmaCare. *PharmaCare Policy Manual*, section 5.5. https://www2.gov.bc.ca/gov/content/health/practitioner-professional-resources/pharmacare/pharmacare-publications/pharmacare-policy-manual-2012
- Population Data BC / Data Innovation Program. https://www2.gov.bc.ca/gov/content/data/finding-and-sharing/data-innovation-program
- OCWA (Output Checking Workflow Application). https://ocwa.popdata.bc.ca

---

## License

Copyright 2026 Bill Warburton


Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
