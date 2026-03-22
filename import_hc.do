/*
import_hc.do
============
Import the Health Canada Drug Product Database (DPD) text files into Stata
format, one .dta file per segment.

This script is called by mme_master.do after the working directory has been
set to $hc_dir.  It expects to find:
  - Five consolidated segment text files:  drug.txt  ingred.txt  route.txt
                                           ther.txt   form.txt
  - Five template .dta files (one per segment) that define the variable names
    for that segment:  drug_template.dta  ingred_template.dta  etc.

The template files hold zero observations but carry the correct variable
names for each segment, which are used to rename the generic v1, v2, ...
columns produced by -import delimited-.

The Health Canada DPD text files use a pipe (|) delimiter with no header row.

Outputs (saved to $hc_dir):
  drug.dta    ingred.dta    route.dta    ther.dta    form.dta

Notes
-----
* All columns are imported as strings first, then drug_code is destringed
  after filtering to numeric values only.
* Rows where drug_code is non-numeric (header artefacts from concatenation)
  are dropped.
* Duplicates are removed within each segment file.

Copyright 2024 Province of British Columbia
Licensed under the Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0
*/

clear all

/* -----------------------------------------------------------------------
   Import each segment in turn
   ----------------------------------------------------------------------- */

local segment_list "drug ingred route ther form"

foreach seg of local segment_list {

    di "Importing segment: `seg' — $(c(current_time))"

    /* Determine variable names from the zero-obs template file */
    use `seg'_template, clear
    unab varlist : *
    local varnames `varlist'
    local nvars : word count `varnames'

    /* Collect all text files for this segment (approved, marketed,
       cancelled, dormant — consolidated by prepare_hc_drug_files.py
       into a single file) */
    local files : dir . files "`seg'*.txt"

    clear
    tempfile master
    save `master', emptyok replace

    foreach f of local files {
        di "  Reading `f'..."
        import delimited using "`f'", ///
            delimiter("|")            ///
            stringcols(_all)          ///
            clear

        /* Guard against files that have more or fewer columns than expected */
        capture confirm variable v`nvars'
        if _rc {
            di as error "  WARNING: `f' has unexpected column count — skipping"
            continue
        }

        rename (v1-v`nvars') (`varnames')
        append using `master'
        save `master', replace
    }

    /* Drop rows where drug_code is not a valid integer (e.g. stray headers) */
    gen _t = real(drug_code)
    keep if !missing(_t)
    drop _t

    duplicates drop
    destring drug_code, replace
    compress

    save "`seg'", replace
    di "  Saved `seg'.dta (${c(N)} records)"
}

di "import_hc complete — $(c(current_time))"
