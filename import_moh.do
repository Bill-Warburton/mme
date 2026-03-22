/*
import_moh.do
=============
Import the BC Ministry of Health opioid list CSV into Stata format.

The MoH list (moh_opioids_drug_list.csv) contains one row per DIN/PIN with
fields for drug name, strength, and route.  It is used later in
opioid_names.do to fill in strength information that is absent from the
Health Canada DPD for DIP-list drugs.

The strength field in the MoH file is a free-text string that may look like:
  "10 MG"     → numeric part extracted as moh_strength = 10
  "10MG"      → numeric part extracted before first "M"
  "5-10 MG"   → range; treated as missing (strpos finds "-")

The original strength string is dropped after extraction; the numeric value
is saved as moh_strength.

Input:   $dir\moh_opioids_drug_list.csv
Output:  $dir\moh_opioid_list.dta

Note: The CSV uses ISO-8859-9 encoding (Turkish), which is the encoding used
by the MoH for this extract.  Stata's -import delimited- encoding() option
is used to handle this correctly.

Copyright 2024 Province of British Columbia
Licensed under the Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0
*/

/* -----------------------------------------------------------------------
   Import
   ----------------------------------------------------------------------- */

import delimited "$dir\moh_opioids_drug_list.csv", ///
    encoding(ISO-8859-9)                            ///
    clear

/* -----------------------------------------------------------------------
   Tidy variables
   ----------------------------------------------------------------------- */

/* Standardise the DIN/PIN variable name to match the rest of the pipeline */
rename dinpin din_pin

/* Extract numeric strength from the free-text strength field.
   Strategy 1: take digits before the first space  → "10 MG" gives 10
   Strategy 2: take digits before the first "M"    → "10MG"  gives 10
   Ranges (containing "-") are left missing.        → "5-10 MG" gives .  */
gen moh_strength = real(substr(strength, 1, strpos(strength, " ")))

replace moh_strength = real(substr(strength, 1, strpos(strength, "M") - 1)) ///
    if missing(moh_strength) & strpos(strength, "-") == 0

/* Drop the raw string now that we have the numeric value */
drop strength

/* -----------------------------------------------------------------------
   Save
   ----------------------------------------------------------------------- */

save "$dir\moh_opioid_list", replace

di "import_moh complete — $(c(current_time))"
