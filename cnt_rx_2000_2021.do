capture log close 
log using "R:\working\Bill\ppp\cnt_rx_2000_2021.log", replace
global dir = "R:\working\Bill\ppp"



/************************************************************************
gather opioid prescriptions into one file
*********************************************************************/

local txmp_dir = "R:\working\Bill\ppp\pnet\"
local file : dir "`txmp_dir'" files "opioid????.dta"
use "R:\working\Bill\ppp\pnet\opioid2020.dta", clear
save tot_opioid, replace
use tot_opioid, clear
foreach f of local file {
		local fn = "R:\working\Bill\ppp\pnet\"+"`f'"
disp "`f'",c(current_time)
	if "`fn'" != "R:\working\Bill\ppp\pnet\opioid2020.dta"{
		append using  "`fn'"
	}
}
save "R:\working\Bill\ppp\pnet\tot_opioid.dta", replace




/************************************************************************
find median by drug and add to mme
********************************************************************/

use "R:\working\Bill\ppp\pnet\tot_opioid.dta", clear
collapse (median) median_days = dspd_days_sply median_q = dspd_qty (p90) p90_days = dspd_days_sply p90_q = dspd_qty, by(din_pin)
merge 1:1 din_pin using "$dir\dip_hc_opioids"
drop _merge
save "$dir\dip_med_mme", replace

