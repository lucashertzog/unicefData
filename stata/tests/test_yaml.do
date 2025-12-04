* Test script for yaml command
* Tests the unified yaml command with subcommands: read, write, describe, list, frames, clear
* Default behavior: uses current dataset
* Frame option: requires explicit frame(name) - Stata 16+ only
clear all
set more off

* Add ado path (parent directory contains src/y/)
adopath + "../src/y"

di as text "{hline 70}"
di as text "{bf:TEST 1: Read YAML into current dataset (default)}"
di as text "{hline 70}"

yaml read using "test_config.yaml", replace verbose

di as text ""
di as text "Dataset contents:"
list in 1/10, clean noobs

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 2: Read YAML with locals option}"
di as text "{hline 70}"

yaml read using "test_config.yaml", locals replace verbose

di as text ""
di as text "Returned values:"
return list

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 3: Display YAML structure (from dataset)}"
di as text "{hline 70}"

yaml describe

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 4: List YAML contents (from dataset)}"
di as text "{hline 70}"

yaml list

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 5: Read with scalars}"
di as text "{hline 70}"

yaml read using "test_config.yaml", scalars replace verbose

di as text ""
di as text "Scalars created:"
scalar list

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 6: Write locals to YAML}"
di as text "{hline 70}"

* Create some locals to write
local project "Test Project"
local version "2.0"
local author "UNICEF"
local year 2025

yaml write using "test_output.yaml", locals(project version author year) replace verbose

di as text ""
di as text "File written. Reading back:"
type "test_output.yaml"

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 7: Write dataset to YAML}"
di as text "{hline 70}"

* Read config first
yaml read using "test_config.yaml", replace

* Write to new file
yaml write using "test_output2.yaml", replace verbose

di as text ""
di as text "File written. Contents:"
type "test_output2.yaml"

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 8: List with keys option (space-delimited)}"
di as text "{hline 70}"

* Get all indicator codes as space-delimited list
yaml list indicators, keys children

di as text ""
di as text "Keys returned: `r(keys)'"

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 9: List with stata option (compound quotes)}"
di as text "{hline 70}"

* Get indicator codes in Stata compound quote format
yaml list indicators, keys children stata

di as text ""
di as text "Keys in Stata format:"
di as result `"`r(keys)'"'

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 10: Loop over indicator codes}"
di as text "{hline 70}"

* Get the keys
yaml list indicators, keys children stata
local indicator_list `"`r(keys)'"'

* Loop over them
di as text "Looping over indicators:"
foreach ind in `indicator_list' {
    di as text "  - Processing indicator: " as result "`ind'"
}

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 11: Get dataflow codes}"
di as text "{hline 70}"

yaml list dataflows, keys children stata
di as text "Dataflow codes: " as result `"`r(keys)'"'

di as text ""
di as text "{hline 70}"
di as text "{bf:TEST 12: Clear dataset}"
di as text "{hline 70}"

yaml clear
di as text "After clear (N=" _N ")"

* Check if Stata version supports frames
if (`c(stata_version)' >= 16) {
    
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:TEST 13: Read YAML into frame (Stata 16+)}"
    di as text "{hline 70}"
    
    * First load some other data so we can show frames don't destroy it
    sysuse auto, clear
    di as text "Current dataset: auto (N=" _N ")"
    
    * Load YAML into a frame
    yaml read using "test_config.yaml", frame(cfg) verbose
    
    di as text ""
    di as text "After loading yaml to frame, current dataset still has N=" _N " (auto)"
    
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:TEST 14: List YAML frames}"
    di as text "{hline 70}"
    
    yaml frames, detail
    
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:TEST 15: Describe from frame}"
    di as text "{hline 70}"
    
    yaml describe, frame(cfg)
    
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:TEST 16: List from frame}"
    di as text "{hline 70}"
    
    yaml list indicators, frame(cfg) keys children
    di as text "Keys: `r(keys)'"
    
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:TEST 17: Multiple frames}"
    di as text "{hline 70}"
    
    * Create a second test file
    capture file close myfile
    file open myfile using "test_config2.yaml", write replace
    file write myfile "name: Second Config" _n
    file write myfile "settings:" _n
    file write myfile "  debug: true" _n
    file write myfile "  max_obs: 5000" _n
    file close myfile
    
    * Read it into another frame
    yaml read using "test_config2.yaml", frame(cfg2) verbose
    
    di as text ""
    di as text "Now we have multiple YAML frames:"
    yaml frames, detail
    
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:TEST 18: Clear specific frame}"
    di as text "{hline 70}"
    
    yaml clear cfg2
    
    di as text ""
    di as text "After clearing cfg2:"
    yaml frames
    
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:TEST 19: Clear all frames}"
    di as text "{hline 70}"
    
    * Reload both
    yaml read using "test_config.yaml", frame(cfg)
    yaml read using "test_config2.yaml", frame(cfg2)
    
    di as text "Before clearing all:"
    yaml frames
    
    yaml clear, all
    
    di as text ""
    di as text "After clearing all:"
    yaml frames

}
else {
    di as text ""
    di as text "{hline 70}"
    di as text "{bf:SKIPPING FRAME TESTS: Stata version < 16}"
    di as text "{hline 70}"
}

di as text ""
di as text "{hline 70}"
di as result "{bf:ALL TESTS COMPLETED}"
di as text "{hline 70}"
