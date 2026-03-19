
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
keep din_pin st2 route op_name strength
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
	gen mme = 0
	replace mme = dspd_qty * st2 if route !=2
	replace mme = dspd_days_sply * st2 if route ==2

	local fn = "R:\working\Bill\ppp\pnet\"+"opioid`yr'"

	quietly compress
	quietly save `fn', replace

}

