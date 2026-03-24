
clear all
clear
capture log close
log using "R:\working\Bill\ppp\cnt_rx.log", replace
global dir = "R:\working\Bill\ppp"


/************************************************************************
count opioid prescriptions
*********************************************************************/
use  "$dir\dip_hc_opioids", clear
gen st2 = factor*cf
encode opioid_name, gen(op_name)
keep din_pin st2 route op_name strength _atlas_match oralmorphineequivalentfactormg
compress
save temp_mme, replace

/*
use "R:\working\Bill\neat\data\demog.dta" , clear
sort studyid
save txmp, replace
*/

local txmp_dir = "R:\working\Bill\ppp\pnet\"
forvalues yr = 2000/2020{
disp "`yr'",c(current_time)
import delimited "R:\\working\\Bill\\ppp\pnet\\`yr'.csv"	, clear
	quietly merge m:1 din_pin using  temp_mme, keep(match) nogenerate
	/* MME calculation — four combinations of opioid list × conversion factor.
	   Variable suffix key:
	     first letter  — opioid list:        a = Atlas,    m = MME/standard
	     second letter — conversion factor:  a = Atlas CF, m = MME/standard CF
	   Atlas CF always multiplies by dspd_qty (patches included).
	   Standard CF multiplies patches by dspd_days_sply.
	   Variables are missing (not zero) when the condition is not met.      */

	/* mme_mm: MME list + MME CF  (standard calculation, all opioids)      */
	gen mme_mm = dspd_qty       * st2 if route != 2
	replace mme_mm = dspd_days_sply * st2 if route == 2

	/* mme_am: Atlas list + MME CF  (standard CF, Atlas-matched opioids only) */
	gen mme_am = dspd_qty       * st2 if _atlas_match & route != 2
	replace mme_am = dspd_days_sply * st2 if _atlas_match & route == 2

	/* mme_ma: MME list + Atlas CF  (Atlas factor where available, all opioids) */
	gen mme_ma = dspd_qty * oralmorphineequivalentfactormg ///
	    if !missing(oralmorphineequivalentfactormg)

	/* mme_aa: Atlas list + Atlas CF  (Atlas factor, Atlas-matched opioids only) */
	gen mme_aa = dspd_qty * oralmorphineequivalentfactormg ///
	    if _atlas_match & !missing(oralmorphineequivalentfactormg)

	/* NOTE: mme_ma and mme_aa are currently identical because
	   oralmorphineequivalentfactormg is only available for Atlas-matched
	   din_pins; it is missing for all others, so the !missing() condition
	   in mme_ma implicitly restricts to the Atlas list anyway.
	   Future enhancement: impute oralmorphineequivalentfactormg for
	   non-Atlas opioids (e.g. from factor × cf scaled to oral morphine
	   equivalents) so that mme_ma genuinely covers the full MME list.    */

	local fn = "R:\working\Bill\ppp\pnet\"+"opioid`yr'"

	quietly compress
	quietly save `fn', replace

}

