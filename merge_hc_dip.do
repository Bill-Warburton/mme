

/**********************************************************************
drug doesn't have duplicates
ingred has mulutiple records per din_pin
route has multiple routed per drug
ther has multiple atc per drug
**********************************************************************/
use "$hc_dir\ther", clear
keep drug_code atc
duplicates drop
tempfile tf
save `tf', replace

/// reduce the number of multiples by 
use "$hc_dir\route", clear
gen t = (strpos( route ,"DISINFECTANT")> 0)
duplicates tag drug_code, gen(dup_tag)
drop if dup_tag > 0 & t
encode route, gen(route2)
keep drug_code route2
duplicates drop
tempfile tf2
save `tf2', replace

use "$hc_dir\ingred", clear
keep drug_code ingredient strength strength_unit dosage_unit dosage_value
duplicates drop
tempfile tf3
save `tf3', replace

use "$hc_dir\form", clear
keep drug_code form_code form
drop if drug_code == 16843 & form !="LIQUID"
collapse (firstnm) form, by(drug_code)
tempfile tf4
save `tf4', replace


use "$hc_dir\drug", clear
rename drug_identification_number din_pin
gen t = real( din_pin)
drop if missing(t)
destring din_pin, replace

keep drug_code din_pin
merge 1:1 drug_code using `tf', keep(match) nogenerate
merge 1:1 drug_code using `tf4', keep(match) nogenerate
merge 1:m drug_code using `tf3', keep(match) nogenerate
destring dosage_value, replace
destring strength, replace
replace dosage_value = 0 if missing(dosage_value)
/*************************
get rid of extraneous drug_codes
****************************/
collapse (max) drug_code dosage_value (firstnm) form, by(dosage_unit strength_unit strength ingredient atc din_pin )
merge m:m drug_code using `tf2', keep(match) nogenerate

save "$dir\hc_drugs", replace
merge m:1 din_pin using "$dir\dip_drugs"

replace ingredient =  gen_drug if _merge == 2
drop _merge

save "$dir\dip_hc", replace


