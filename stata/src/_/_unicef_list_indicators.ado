*******************************************************************************
* _unicef_list_indicators.ado
*! v 1.2.0   09Dec2025               by Joao Pedro Azevedo (UNICEF)
* List UNICEF indicators for a specific dataflow using YAML metadata
* Uses yaml.ado for robust YAML parsing
*******************************************************************************

program define _unicef_list_indicators, rclass
    version 14.0
    
    syntax , Dataflow(string) [VERBOSE METApath(string)]
    
    quietly {
    
        *-----------------------------------------------------------------------
        * Locate metadata directory (YAML files in src/_/ alongside this ado)
        *-----------------------------------------------------------------------
        
        if ("`metapath'" == "") {
            * Find the helper program location (src/_/)
            capture findfile _unicef_list_indicators.ado
            if (_rc == 0) {
                local ado_path "`r(fn)'"
                * Extract directory containing this ado file
                local ado_dir = subinstr("`ado_path'", "\", "/", .)
                local ado_dir = subinstr("`ado_dir'", "_unicef_list_indicators.ado", "", .)
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
            noi di as text "Reading indicators from: " as result "`yaml_file'"
        }
        
        *-----------------------------------------------------------------------
        * Read YAML file and filter by dataflow
        *-----------------------------------------------------------------------
        
        preserve
        
        yaml read using "`yaml_file'", replace
        
        * Get all indicator codes under 'indicators' parent
        yaml list indicators, keys children
        local all_indicators "`r(keys)'"
        
        * Filter to those matching the specified dataflow
        local dataflow_upper = upper("`dataflow'")
        local matches ""
        local match_names ""
        local n_matches = 0
        
        foreach ind of local all_indicators {
            * Get dataflow attribute for this indicator
            capture yaml get indicators:`ind', attributes(dataflow name) quiet
            if (_rc == 0) {
                local ind_df = upper("`r(dataflow)'")
                if ("`ind_df'" == "`dataflow_upper'") {
                    local ++n_matches
                    local matches "`matches' `ind'"
                    local match_names `"`match_names' "`r(name)'""'
                }
            }
        }
        
        local matches = strtrim("`matches'")
        
        restore
        
    } // end quietly
    
    *---------------------------------------------------------------------------
    * Display results
    *---------------------------------------------------------------------------
    
    noi di ""
    noi di as text "{hline 70}"
    noi di as text "Indicators in Dataflow: " as result "`dataflow_upper'"
    noi di as text "{hline 70}"
    noi di ""
    
    if (`n_matches' == 0) {
        noi di as text "  No indicators found for dataflow '`dataflow_upper'"
        noi di as text "  Use 'unicefdata, flows' to see available dataflows."
    }
    else {
        noi di as text _col(2) "{ul:Indicator}" _col(25) "{ul:Name}"
        noi di ""
        
        forvalues i = 1/`n_matches' {
            local ind : word `i' of `matches'
            local nm : word `i' of `match_names'
            
            * Truncate name if too long
            if (length("`nm'") > 45) {
                local nm = substr("`nm'", 1, 42) + "..."
            }
            
            noi di as result _col(2) "`ind'" as text _col(25) "`nm'"
        }
    }
    
    noi di ""
    noi di as text "{hline 70}"
    noi di as text "Total: " as result `n_matches' as text " indicator(s) in `dataflow_upper'"
    noi di as text "{hline 70}"
    
    *---------------------------------------------------------------------------
    * Return values
    *---------------------------------------------------------------------------
    
    return scalar n_indicators = `n_matches'
    return local indicators "`matches'"
    return local dataflow "`dataflow_upper'"
    
end
