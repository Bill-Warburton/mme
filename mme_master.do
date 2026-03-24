



clear all
clear
capture log close
global dir = "R:\working\Bill\ppp"
log using "$dir\mme_master.log", replace
global data_dir = "R:\working\Bill\ppp\data"
global hc_dir = "R:\working\Bill\ppp\hc"
global data_version = "R:\DATA\2026-03-05"

/* Atlas option
   restrict_to_atlas – set to 1 to keep only din_pins that appear in the
                       Atlas file; drops all other opioids from the pipeline.
                       When 0 (default), strip_opioids.do produces all four
                       MME variants: mme_mm, mme_am, mme_ma, mme_aa.        */
global restrict_to_atlas 0

cd  "$dir"

/***********************************************************
copy selected fields from the pharmanet data
***********************************************************/
python script strip_pnet_pq.py

/***********************************************************
extract latest list of drugs
***********************************************************/

import delimited "$data_version\Ministry_of_Health\PharmaNet\csv\pharmanet-hlth-prod_2026_20260107.csv", varnames(1) clear 
gen t = real(din_pin)
keep if !missing(t)
destring din_pin, replace
drop t
save "$dir\dip_drugs", replace


/***********************************************************
import the files from Health Canada
***********************************************************/
capture log close
cd $hc_dir
log using import_hc, replace
do import_hc



/***********************************************************
keep relevant variables from hc files
remove duplicates
merge HC file and add to dip 
Because some drugs have multiple ingredients and routes
there are many duplicate din_pin's in the resulting file
***********************************************************/

cd  "$dir"
capture log close
log using merge_hc_dip, replace
do merge_hc_dip.do




/***********************************************************
import list of opioids and their conversion factors from moh
***********************************************************/
capture log close
log using import_moh, replace
do import_moh.do




/***********************************************************
identify opioids
reduce to 1 record per din_pin
***********************************************************/
cd  "$dir"
capture log close
log using opioid_names, replace
do opioid_names.do



/***********************************************************
strip the opioids from the downloaded pharmanet files
***********************************************************/
capture log close
log using strip_opioids, replace
do strip_opioids.do


/***********************************************************
count the prescr
***********************************************************/
capture log close
log using strip_opioids, replace
do strip_opioids.do


