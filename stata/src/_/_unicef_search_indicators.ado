*******************************************************************************
* _unicef_search_indicators.ado
*! v 1.3.0   17Dec2025               by Joao Pedro Azevedo (UNICEF)
* Search UNICEF indicators by keyword using YAML metadata
* Uses yaml.ado for robust YAML parsing
* Uses Stata frames (v16+) for better isolation when available
*******************************************************************************

program define _unicef_search_indicators, rclass
    version 14.0
    
    syntax , Keyword(string) [Limit(integer 20) VERBOSE METApath(string)]
    
    * Check if frames are available (Stata 16+)
    local use_frames = (c(stata_version) >= 16)
    
    quietly {
    
        *-----------------------------------------------------------------------
        * Locate metadata directory (YAML files in src/_/ alongside this ado)
        *-----------------------------------------------------------------------
        
        if ("`metapath'" == "") {
            * Find the helper program location (src/_/)
            capture findfile _unicef_search_indicators.ado
            if (_rc == 0) {
                local ado_path "`r(fn)'"
                * Extract directory containing this ado file
                local ado_dir = subinstr("`ado_path'", "\", "/", .)
                local ado_dir = subinstr("`ado_dir'", "_unicef_search_indicators.ado", "", .)
                local metapath "`ado_dir'"
            }
            
            * Fallback to PLUS directory _/
            if ("`metapath'" == "") | (!fileexists("`metapath'_unicefdata_indicators.yaml")) {
                local metapath "`c(sysdir_plus)'_/"
            }
        }
        
        local yaml_file "`metapath'_unicefdata_indicators.yaml"
        
        *-----------------------------------------------------------------------
        * Check YAML file exists
        *-----------------------------------------------------------------------
        
        capture confirm file "`yaml_file'"
        if (_rc != 0) {
            noi di as err "Indicators metadata not found at: `yaml_file'"
            noi di as err "Run 'unicefdata_sync' to download metadata."
            exit 601
        }
        
        if ("`verbose'" != "") {
            noi di as text "Searching indicators in: " as result "`yaml_file'"
        }
        
        *-----------------------------------------------------------------------
        * Read YAML file and search (frames for Stata 16+)
        *-----------------------------------------------------------------------
        
        * Search in both code and name
        local keyword_lower = lower("`keyword'")
        local matches ""
        local match_names ""
        local match_dataflows ""
        local n_matches = 0
        
        if (`use_frames') {
            * Stata 16+ - use frames for better isolation
            local yaml_frame "_unicef_yaml_temp"
            capture frame drop `yaml_frame'
            
            * Read YAML into a frame
            yaml read using "`yaml_file'", frame(`yaml_frame')
            
            * Work within the frame
            frame `yaml_frame' {
                * Get all indicator codes under 'indicators' parent
                yaml list indicators, keys children frame(`yaml_frame')
                local all_indicators "`r(keys)'"
                
                foreach ind of local all_indicators {
                    * Check if keyword matches indicator code
                    local ind_lower = lower("`ind'")
                    local found = 0
                    
                    if (strpos("`ind_lower'", "`keyword_lower'") > 0) {
                        local found = 1
                    }
                    
                    * Also check name
                    if (`found' == 0) {
                        capture yaml get indicators:`ind', attributes(name) quiet frame(`yaml_frame')
                        if (_rc == 0 & "`r(name)'" != "") {
                            local name_lower = lower("`r(name)'")
                            if (strpos("`name_lower'", "`keyword_lower'") > 0) {
                                local found = 1
                            }
                        }
                    }
                    
                    if (`found' == 1) {
                        local ++n_matches
                        local matches "`matches' `ind'"
                        
                        * Get name and dataflow for this indicator
                        capture yaml get indicators:`ind', attributes(name dataflow) quiet frame(`yaml_frame')
                        if (_rc == 0) {
                            local match_names `"`match_names' "`r(name)'""'
                            local match_dataflows "`match_dataflows' `r(dataflow)'"
                        }
                        else {
                            local match_names `"`match_names' "N/A""'
                            local match_dataflows "`match_dataflows' N/A"
                        }
                        
                        if (`n_matches' >= `limit') {
                            continue, break
                        }
                    }
                }
            }
            
            * Clean up frame
            capture frame drop `yaml_frame'
        }
        else {
            * Stata 14/15 - use preserve/restore
            preserve
            
            yaml read using "`yaml_file'", replace
            
            * Get all indicator codes under 'indicators' parent
            yaml list indicators, keys children
            local all_indicators "`r(keys)'"
            
            foreach ind of local all_indicators {
                * Check if keyword matches indicator code
                local ind_lower = lower("`ind'")
                local found = 0
                
                if (strpos("`ind_lower'", "`keyword_lower'") > 0) {
                    local found = 1
                }
                
                * Also check name
                if (`found' == 0) {
                    capture yaml get indicators:`ind', attributes(name) quiet
                    if (_rc == 0 & "`r(name)'" != "") {
                        local name_lower = lower("`r(name)'")
                        if (strpos("`name_lower'", "`keyword_lower'") > 0) {
                            local found = 1
                        }
                    }
                }
                
                if (`found' == 1) {
                    local ++n_matches
                    local matches "`matches' `ind'"
                    
                    * Get name and dataflow for this indicator
                    capture yaml get indicators:`ind', attributes(name dataflow) quiet
                    if (_rc == 0) {
                        local match_names `"`match_names' "`r(name)'""'
                        local match_dataflows "`match_dataflows' `r(dataflow)'"
                    }
                    else {
                        local match_names `"`match_names' "N/A""'
                        local match_dataflows "`match_dataflows' N/A"
                    }
                    
                    if (`n_matches' >= `limit') {
                        continue, break
                    }
                }
            }
            
            restore
        }
        
        local matches = strtrim("`matches'")
        
    } // end quietly
    
    *---------------------------------------------------------------------------
    * Display results
    *---------------------------------------------------------------------------
    
    noi di ""
    noi di as text "{hline 70}"
    noi di as text "Search Results for: " as result "`keyword'"
    noi di as text "{hline 70}"
    noi di ""
    
    if (`n_matches' == 0) {
        noi di as text "  No indicators found matching '`keyword'"
    }
    else {
        noi di as text _col(2) "{ul:Indicator}" _col(20) "{ul:Dataflow}" _col(35) "{ul:Name}"
        noi di ""
        
        forvalues i = 1/`n_matches' {
            local ind : word `i' of `matches'
            local df : word `i' of `match_dataflows'
            local nm : word `i' of `match_names'
            
            * Truncate name if too long
            if (length("`nm'") > 35) {
                local nm = substr("`nm'", 1, 32) + "..."
            }
            
            noi di as result _col(2) "`ind'" as text _col(20) "`df'" _col(35) "`nm'"
        }
        
        if (`n_matches' >= `limit') {
            noi di ""
            noi di as text "  (Showing first `limit' matches. Use limit() option for more.)"
        }
    }
    
    noi di ""
    noi di as text "{hline 70}"
    noi di as text "Found: " as result `n_matches' as text " indicator(s)"
    noi di as text "{hline 70}"
    
    *---------------------------------------------------------------------------
    * Return values
    *---------------------------------------------------------------------------
    
    return scalar n_matches = `n_matches'
    return local indicators "`matches'"
    return local keyword "`keyword'"
    
end
