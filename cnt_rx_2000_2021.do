/*
cnt_rx_2000_2021.do
===================
Collate all per-year opioid prescription files into a single dataset and
compute summary statistics by drug.

This is the final step of the pipeline.  It:
  1. Appends the per-year opioid .dta files produced by strip_opioids.do
     into a single combined dataset (tot_opioid.dta).
  2. Collapses to one row per din_pin and calculates the median and 90th
     percentile of dspd_days_sply and dspd_qty across all years.
  3. Merges these summary statistics back with the opioid drug reference
     (dip_hc_opioids.dta) to create a final reference file that includes
     both drug characteristics and empirical quantity distributions.

The empirical distributions (medians, 90th percentiles) are used by
downstream analyses to:
  - Impute missing or implausible quantities (e.g. replace quantities
    greater than 10× the 90th percentile with the median).
  - Compare observed dispensing patterns against expected ranges.

Inputs
------
  $pnet_dir\opioid<year>.dta   – per-year opioid files (from strip_opioids.do)
  $dir\dip_hc_opioids.dta      – opioid drug reference (from opioid_names.do)

Outputs
-------
  $pnet_dir\tot_opioid.dta     – all opioid prescriptions across all years
  $dir\dip_med_mme.dta         – drug reference + median/p90 quantities

Year range is taken from $start_year and $end_year set in mme_master.do.

Copyright 2024 Province of British Columbia
Licensed under the Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0
*/

/* -----------------------------------------------------------------------
   Step 1: Collate per-year opioid files
   Use a forvalues loop over the global year range so the list of files is
   always consistent with what strip_opioids.do produced.
   ----------------------------------------------------------------------- */

local first_year = $start_year

/* Initialise with the first year */
use "$pnet_dir\opioid`first_year'.dta", clear
local total_n = _N
di "  opioid`first_year'.dta: ${c(N)} records"

/* Append remaining years */
forvalues yr = `=`first_year' + 1' / $end_year {

    local fn = "$pnet_dir\opioid`yr'.dta"
    capture confirm file "`fn'"
    if _rc {
        di "  WARNING: `fn' not found — skipping"
        continue
    }

    di "  Appending `yr' — $(c(current_time))"
    append using "`fn'"
    di "  opioid`yr'.dta: added records; running total = ${c(N)}"
}

compress
save "$pnet_dir\tot_opioid.dta", replace
di "tot_opioid.dta saved: ${c(N)} total opioid prescription records"

/* -----------------------------------------------------------------------
   Step 2: Compute empirical quantity distributions by din_pin
   Median and 90th percentile of days supplied and quantity dispensed.
   These are used for outlier detection and imputation.
   ----------------------------------------------------------------------- */

use "$pnet_dir\tot_opioid.dta", clear

collapse                                            ///
    (median) median_days = dspd_days_sply           ///
             median_q    = dspd_qty                 ///
    (p90)    p90_days    = dspd_days_sply           ///
             p90_q       = dspd_qty,                ///
    by(din_pin)

di "Quantity distributions computed for ${c(N)} unique din_pin values"

/* -----------------------------------------------------------------------
   Step 3: Merge with the opioid drug reference
   This joins drug characteristics (opioid name, route, factor, cf) with
   the empirical distributions, producing a single reference file that can
   be used to calculate MME with optional outlier capping.
   ----------------------------------------------------------------------- */

merge 1:1 din_pin using "$dir\dip_hc_opioids"
drop _merge

save "$dir\dip_med_mme", replace
di "dip_med_mme.dta saved: ${c(N)} records"
di "cnt_rx complete — $(c(current_time))"
