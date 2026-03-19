clear all
clear

/**********************************************************************
import the health canada files 
*********************************************************************/

local type_list "drug ingred route ther form"
foreach d of local type_list {
	use `d'_template, clear
	unab varlist : *
	local varnames  `varlist' 
	local nvars: word count `varnames'
	local files: dir . files "`d'*.txt"
		clear
		tempfile master
		save `master', emptyok replace
	foreach f of local files {
	di "`f'"
		import delimited using "`f'", clear stringcols(_all)
		rename (v1-v`nvars')(`varnames')
		append using  `master'
		save `master', replace

	}
	gen t = real(drug_code)
	keep if !missing(t)
	drop t
	duplicates drop
	destring drug_code , replace
	compress

	save  "`d'", replace
}




