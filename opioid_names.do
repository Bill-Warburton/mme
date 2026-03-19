
/********************************************************************
get list of opioids with din_pin

****************************************************************/

clear
import delimited using $dir\list_of_opioids.txt
gen t = length(v1)
sort t 
local druglist ""
forval i =  1/`=_N' {
	local druglist "`druglist' `=v1[`i']'"
}

disp "`druglist'"
// no DIHYDROCODEINE in either dip or HC data.  Not in HC drug product database online

local not_opioid  APOMORPHINE IPRATROPIUM TIOTROPIUM HELIOTROPIUM 

use $dir\dip_hc.dta, clear
gen yikes = 0
gen opioid = 0
gen opioid_name = ""
foreach d of local druglist {
	disp "`d'",c(current_time)
	replace opioid = 1 if (strpos(ingredient,"`d'")> 0) 
	replace yikes = 1 if (strpos(drug_brand_nm,"`d'")> 0) 
	// some drug names are contained within other.  replace if longer
	replace opioid_name = "`d'" if (strpos(ingredient,"`d'")> 0) & (strlen("`d'") > strlen(opioid_name) | missing(opioid_name))
}


foreach  d of local not_opioid {
	replace opioid = 0 if (strpos(ingredient,"`d'")> 0) 
	replace yikes = 0 if (strpos(drug_brand_nm,"`d'")> 0) 
	replace opioid_name = "" if (strpos(ingredient,"`d'")> 0) 
}
keep if opioid | (yikes & ingredient== "Unknown Generic Drug")
save temp, replace

/**********************************************************
manually parse brand name into ingredient strength and strength_unit
a few did not have strength imbedded in brand name
keep if yikes
// 
keep if ingredient== "Unknown Generic Drug"
keep din_pin drug_brand_nm
export excel using "R:\working\Bill\ppp\Parse_brand_name_out.xls", firstrow(variables) replace
 typo hydromorphine for hydromorphone
sinutab with codeine looked up online
import excel "R:\working\Bill\ppp\Parse_brand_name.xls", sheet("Sheet1") firstrow clear
save $dir\parse_brand_name, replace
export delimited using "$dir\parse_brand_name.csv"
***************************************************************/

$dir\parse_brand_name, clear
gen opioid_name = ingredient
gen opioid = 1
gen dosage_value=0
joinby din_pin using temp, unmatched(using) update
drop _merge
save temp, replace

/// the update option up replaces missing values in Parse_brand_name
/// with values from temp.  Parse_brand_name is a subset of temp, so 

/**********************************************************
manually parse strength into  strength and strength_unit
where it is missing
keep if strength==0 | missing(strength)
keep din_pin drug_brand_nm gen_drug_strgth gen_dsg_form gen_entry_route_dscr
export excel using "R:\working\Bill\ppp\Parse_strength_out.xls", firstrow(variables) replace

had to look up about 1/3 online.  Some ambiguity
import excel "R:\working\Bill\ppp\Parse_strength.xls", sheet("Sheet1") firstrow clear
save $dir\Parse_strength, replace
export delimited $dir\Parse_strength, replace
***************************************************************/

import delimited  $dir\Parse_strength, clear
joinby din_pin using temp, unmatched(using) update
replace strength= gen_drug_strgth if missing(strength) | strength == 0



disp ""
tab drug_brand_nm if strength==0 | missing(strength)


/**************************************************************
 gen_drug_strgth gen_dsg_form gen_entry_route_dscr not filled in 
 if din_pin > 10000000 
 
****************************************************************/


/*********************************************************
to calculate mme need
	strength units
	opioid name
	route: 
		oral/rectal
		patch
		injected
		
to calculate quantity need
	form
		tablet
		liquid
		patch
		spray
	route:
		oral 
		injected
		patch
		
	
**********************************************************/

replace route2=0 if missing(route2)
gen route = 0
replace route = 1 if ///
	(strpos( gen_entry_route_dscr ,"ORAL")> 0) | ///
	(strpos( gen_entry_route_dscr ,"RECTAL")> 0) | ///
	(strpos( gen_dsg_form ,"TAB")> 0) | ///
	(strpos( drug_brand_nm ,"TAB")> 0) | ///
	(strpos( drug_brand_nm ,"SUPPOSITORY")> 0) | ///
	(strpos( drug_brand_nm ,"ORAL")> 0) | ///
	(strpos( drug_brand_nm ,"COUGH")> 0) | ///
	inlist(route2, 73, 4, 79, 86) 
	
replace route = 2 if ///
	inlist(route2, 98) | ///
	(strpos( drug_brand_nm ,"PATCH")> 0) 

/// tab in injectable converted here	
replace route = 3 if ///
	inlist(route2, 14, 45, 59, 84) | ///
	strpos( drug_brand_nm ,"INJ")> 0 | ///
	strpos( drug_brand_nm ,"INTRA")> 0 | ///
	strpos( gen_entry_route_dscr ,"INJ")> 0	
	
replace route = 4 if ///
	inlist(route2, 68) | ///
	(strpos( gen_entry_route_dscr ,"NASAL")> 0) | ///
	(strpos( drug_brand_nm ,"SPRAY")> 0) 
	
replace route = 5 if ///
	inlist(route2, 86, 4) | ///
	(strpos( drug_brand_nm ,"BUCCAL")> 0) | ///
	(strpos( drug_brand_nm ,"SUBLINGUAL")> 0) 


// methadone oral	
replace route = 1 if (strpos( drug_brand_nm ,"METHADONE")> 0) & route == 0
replace route = 1 if (strpos( drug_brand_nm ,"CAP")> 0) & route == 0
replace route = 1 if (strpos( drug_brand_nm ,"SUP")> 0) & route == 0

/****************************************************************************************
 checked MoH file.  No additional information on strength, route or form

	
drop _merge
merge m:1 din_pin using "R:\working\Bill\ppp\moh_opioid_list.dta"	
drop if _merge == 2	
********************************************************************************************/	




/****************************************************************************************
 HYDROMORPHONE  OR MORPHINE OR FENTANYL  + BUPIVICAINE 35MG/ML INECTABLE (INTRATHECAL) FROM ONLINE SEARCH
also 
tab 
route if (strpos( drug_brand_nm ,"HYDROMORPHONE")> 0 | strpos( drug_brand_nm ,"MORPHINE")> 0 | strpos( drug_brand_nm ,"FENTANYL")> 0) & (strpos( drug_brand_nm ,"BUPIVICAINE")> 0 |strpos( drug_brand_nm ,"BUPIVACAINE")> 0)

found 4 drugs with these combinations--all injectable
********************************************************************************************/

replace route = 3 if  route==0 & (strpos( drug_brand_nm ,"HYDROMORPHONE")> 0 | strpos( drug_brand_nm ,"MORPHINE")> 0 | strpos( drug_brand_nm ,"FENTANYL")> 0) & (strpos( drug_brand_nm ,"BACLOFEN")> 0 | strpos( drug_brand_nm ,"CLONIDINE")> 0 | strpos( drug_brand_nm ,"BUPIVICAINE")> 0 | strpos( drug_brand_nm ,"BUPIVACAINE")> 0)
	
/*********************************************************
by inspection, last 19 are injectable
**********************************************************/	
replace route = 3 if  route==0



	
/*********************************************************
see if vial or syringe
by inspection there are no prefilled syringes of opioids
gen t = strpos( drug_brand_nm ,"SYR")> 0 & route == 3
tab t
**********************************************************/
gen vial = strpos( drug_brand_nm ,"VIAL")> 0 | dosage_unit=="VIAL"


	
/*********************************************************
looked up anomalous strength units online
**********************************************************/

replace strength_unit = drug_strength_unit if !missing(drug_strength_unit)

replace  dosage_unit ="ML" if din_pin == 51330
replace  strength_unit ="MG" if din_pin == 51330
replace  strength =.05 if din_pin == 51330

replace  dosage_unit ="ML" if din_pin == 179930
replace  strength_unit ="MG" if din_pin == 179930
replace  strength =2 if din_pin == 179930

replace  dosage_unit ="ML" if din_pin == 50830
replace  strength_unit ="MG" if din_pin == 50830
/// opium
replace  strength_unit ="MG" if din_pin == 95680

/// those RAN Fentanyl patches
replace  strength =  25  if din_pin ==  2249391
replace  strength =  50  if din_pin ==  2249413
replace  strength =  75  if din_pin ==  2249421
replace  strength =  100  if din_pin ==  2249448

replace strength_unit = "MCG" if inlist(din_pin, 2249391, 2249413, 2249421, 2249448)



/*********************************************************
gen a variable that gives mme when multiplied by conversion 
factor and quantity_supplied or days supplied
**********************************************************/	

gen factor = strength

replace factor = strength/dosage_value ///
	if (route==1 | route ==3) & dosage_value !=0  & !missing(dosage_value)  // oral or inj
replace factor = strength if route ==3 & vial // inj and vial or syringe
replace factor = strength if inlist(din_pin, 2483084, 2483092, 9858127, 9858128 ) 
// specific drugs listed in pharmacare quantitites policy # syringes
replace factor = 1.7 if din_pin == 2113031

/// policy manual says # doses. from web, 1 dose  = 1.7 mg

/*********************************************************
tidy mme

save temp, replace 
import delimited "R:\working\Bill\mme\mme.csv", clear 
keep med_name units mme_route cf
rename med_name opioid_name
replace opioid_name = upper(opioid_name)
rename units strength_unit
replace strength_unit = upper(strength_unit)
duplicates drop
export delimited $dir\mme, replace
save  $dir\mme, replace
use temp, clear
**********************************************************/

/*********************************************************
reduce to one record per din_pin
**********************************************************/
keep din_pin opioid_name factor route strength_unit strength hc_or_dip
duplicates drop

gen mme_route = route
replace mme_route = 1 if !inlist(opioid_name,"BUPRENORPHINE" ,"FENTANYL","BUTORPHANOL" )
save temp, replace
clear
import delimited  $dir\mme
save  $dir\mme, replace
use temp, clear

merge m:1 opioid_name mme_route strength_unit using $dir\mme
drop _merge
drop if missing(factor)
save "$dir\dip_hc_opioids", replace

