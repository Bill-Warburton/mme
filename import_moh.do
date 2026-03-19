


import delimited "R:\working\Bill\ppp\moh_opioids_drug_list.csv", encoding(ISO-8859-9) clear
rename dinpin din_pin

gen moh_strength = real(substr(strength,1,strpos(strength," ")))
replace  moh_strength = real(substr(strength,1,strpos(strength,"M")-1)) if missing(moh_strength) & strpos(strength,"-")==0
drop strength

save $dir\moh_opioid_list, replace
