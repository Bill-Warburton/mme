/*
merge_hc_dip.do
===============
Build a unified drug reference file by merging the Health Canada Drug
Product Database (DPD) segments with the BC DIP drug list (hlth_prod).

Background on the DPD segment structure
----------------------------------------
  drug    – one row per drug_code; carries drug_identification_number (DIN/PIN)
  ingred  – one-to-many: multiple ingredients per drug_code
  route   – one-to-many: multiple routes per drug_code
  ther    – one-to-many: multiple ATC codes per drug_code
  form    – one-to-many: multiple forms per drug_code (reduced to one below)

Because a single drug can have multiple ingredients and routes, the merged
file inevitably contains multiple rows per din_pin.  This is expected and
handled downstream in opioid_names.do.

Merge strategy
--------------
1.  Prepare four lookup tempfiles (ther, route, ingred, form), applying
    minimal cleaning to each.
2.  Start from the drug segment (one row per drug_code ≈ one per DIN/PIN)
    and join the four lookup files.
3.  Collapse form to one row per drug_code (firstnm).
4.  Merge the result with the DIP drug list (dip_drugs.dta) using m:m on
    din_pin to pick up DIP-only drugs (those absent from the HC DPD, likely
    compounded products).

Outputs
-------
  $dir\hc_drugs.dta   – DPD drugs with ingredients, routes, ATC codes, forms
  $dir\dip_hc.dta     – hc_drugs merged with dip_drugs; ingredient filled in
                         from gen_drug for DIP-only records

Notes on specific cleaning steps
---------------------------------
* Route: "DISINFECTANT" rows are dropped for drugs that also have at least
  one non-DISINFECTANT route, to avoid spurious route assignments.
* Form: drug_code 16843 has multiple forms; only the LIQUID form is kept
  because the injectable version is the only one that appears in PharmaNet.
* dosage_value: missing values are set to 0 so that the factor calculation
  in opioid_names.do (which divides strength by dosage_value) can use a
  simple `dosage_value != 0` guard.

Copyright 2024 Province of British Columbia
Licensed under the Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0
*/

/* -----------------------------------------------------------------------
   1. Therapeutic classification (ATC codes)
      One ATC code per drug_code after duplicates drop — keep only what
      we need for the opioid search (ATC prefix N02A).
   ----------------------------------------------------------------------- */

use "$hc_dir\ther", clear
keep drug_code atc
duplicates drop
tempfile tf_ther
save `tf_ther', replace

/* -----------------------------------------------------------------------
   2. Route of administration
      A drug may have several routes.  We remove rows labelled DISINFECTANT
      only when other routes exist for the same drug (so a drug whose only
      route is DISINFECTANT is not silently dropped).
      We then encode route as a numeric for downstream comparisons.
   ----------------------------------------------------------------------- */

use "$hc_dir\route", clear

/* Flag DISINFECTANT rows */
gen _disinfectant = (strpos(route, "DISINFECTANT") > 0)

/* Tag drugs that have more than one route record */
duplicates tag drug_code, gen(_dup_tag)

/* Drop DISINFECTANT rows only when the drug has other routes */
drop if _dup_tag > 0 & _disinfectant
drop _disinfectant _dup_tag

encode route, gen(route2)
keep drug_code route2
duplicates drop

tempfile tf_route
save `tf_route', replace

/* -----------------------------------------------------------------------
   3. Ingredients, strengths, and dosage values
   ----------------------------------------------------------------------- */

use "$hc_dir\ingred", clear
keep drug_code ingredient strength strength_unit dosage_unit dosage_value
duplicates drop
tempfile tf_ingred
save `tf_ingred', replace

/* -----------------------------------------------------------------------
   4. Pharmaceutical form
      Collapse to one row per drug_code.  For drug_code 16843 (a product
      that exists in both tablet and liquid form) keep only LIQUID because
      that is the form recorded in PharmaNet for this product.
   ----------------------------------------------------------------------- */

use "$hc_dir\form", clear
keep drug_code form_code form
drop if drug_code == 16843 & form != "LIQUID"
collapse (firstnm) form, by(drug_code)
tempfile tf_form
save `tf_form', replace

/* -----------------------------------------------------------------------
   5. Build the merged HC drug file, starting from the drug segment
   ----------------------------------------------------------------------- */

use "$hc_dir\drug", clear

/* Rename DIN field to match the rest of the pipeline */
rename drug_identification_number din_pin

/* Drop rows where din_pin is not a valid integer */
gen _t = real(din_pin)
drop if missing(_t)
drop _t
destring din_pin, replace

keep drug_code din_pin

/* Join ATC codes and form (1:1 on drug_code) */
merge 1:1 drug_code using `tf_ther', keep(match) nogenerate
merge 1:1 drug_code using `tf_form', keep(match) nogenerate

/* Join ingredients (1:m on drug_code — expands rows) */
merge 1:m drug_code using `tf_ingred', keep(match) nogenerate

/* Convert dosage_value and strength to numeric */
destring dosage_value, replace
destring strength,     replace

/* Set missing dosage_value to 0 so the factor formula works cleanly */
replace dosage_value = 0 if missing(dosage_value)

/* Collapse to remove redundant drug_code rows that arose from the join.
   For each unique combination of key opioid fields, keep the maximum
   dosage_value and the first-seen form.                                   */
collapse                                        ///
    (max)     drug_code dosage_value            ///
    (firstnm) form,                             ///
    by(dosage_unit strength_unit strength ingredient atc din_pin)

/* Now join route (m:m because multiple routes may exist per drug_code) */
merge m:m drug_code using `tf_route', keep(match) nogenerate

save "$dir\hc_drugs", replace
di "  hc_drugs saved (${c(N)} records)"

/* -----------------------------------------------------------------------
   6. Merge with the DIP drug list
      _merge==1: HC drugs not in DIP (not dispensed in BC in study period)
      _merge==2: DIP drugs not in HC (likely compounded products)
      _merge==3: matched on both sides
      For DIP-only records, fill ingredient from gen_drug.
   ----------------------------------------------------------------------- */

merge m:1 din_pin using "$dir\dip_drugs"

/* For DIP-only records (no HC ingredient available), use the DIP
   generic drug name as a fallback ingredient for opioid name searching */
replace ingredient = gen_drug if _merge == 2

drop _merge

save "$dir\dip_hc", replace
di "  dip_hc saved (${c(N)} records)"
di "merge_hc_dip complete — $(c(current_time))"
