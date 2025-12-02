# Load required packages for pipe operator
#' @import dplyr
#' @importFrom magrittr %>%
NULL

# Ensure required packages are loaded when sourcing directly
if (!requireNamespace("magrittr", quietly = TRUE)) {
  stop("Package 'magrittr' is required. Install with: install.packages('magrittr')")
}
if (!requireNamespace("dplyr", quietly = TRUE)) {
  stop("Package 'dplyr' is required. Install with: install.packages('dplyr')")
}
if (!requireNamespace("purrr", quietly = TRUE)) {
  stop("Package 'purrr' is required. Install with: install.packages('purrr')")
}
if (!requireNamespace("httr", quietly = TRUE)) {
  stop("Package 'httr' is required. Install with: install.packages('httr')")
}

# Import pipe operator for direct sourcing
`%>%` <- magrittr::`%>%`

# Internal helper to perform HTTP GET and return text
fetch_sdmx <- function(url, ua, retry) {
  resp <- httr::RETRY("GET", url, ua, times = retry, pause_base = 1)
  httr::stop_for_status(resp)
  httr::content(resp, as = "text", encoding = "UTF-8")
}

#' @title List available UNICEF SDMX “flows”
#' @description
#' Download and cache the SDMX data-flow definitions from UNICEF’s REST endpoint.
#' @return A tibble with columns \code{id}, \code{agency}, and \code{version}
#' @export
list_unicef_flows <- memoise::memoise(
  function(cache_dir = tools::R_user_dir("get_unicef","cache"), retry = 3) {
    ua <- httr::user_agent("get_unicef/1.0 (+https://github.com/jpazvd/get_unicef)")
    url <- "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/dataflow/UNICEF?references=none&detail=full"
    xml_text <- fetch_sdmx(url, ua, retry)
    doc <- xml2::read_xml(xml_text)
    # Use namespace-aware XPath
    ns <- xml2::xml_ns(doc)
    dfs <- xml2::xml_find_all(doc, ".//str:Dataflow", ns)
    tibble::tibble(
      id      = xml2::xml_attr(dfs, "id"),
      agency  = xml2::xml_attr(dfs, "agencyID"),
      version = xml2::xml_attr(dfs, "version")
    )
  }
)

#' @title List SDMX codelist for a given flow + dimension
#' @param flow character flow ID, e.g. "NUTRITION"
#' @param dimension character dimension ID within that flow, e.g. "INDICATOR"
#' @return A tibble with columns \code{code} and \code{description}
#' @export
list_unicef_codelist <- memoise::memoise(
  function(flow, dimension, cache_dir = tools::R_user_dir("get_unicef","cache"), retry = 3) {
    stopifnot(is.character(flow), is.character(dimension), length(flow) == 1, length(dimension) == 1)
    ua <- httr::user_agent("get_unicef/1.0 (+https://github.com/jpazvd/get_unicef)")
    url <- sprintf(
      "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/structure/codelist/UNICEF.%s.%s?references=none&detail=full",
      flow, dimension
    )
    xml_text <- fetch_sdmx(url, ua, retry)
    doc <- xml2::read_xml(xml_text)
    codes <- xml2::xml_find_all(doc, ".//Code")
    tibble::tibble(
      code        = xml2::xml_attr(codes, "value"),
      description = xml2::xml_text(xml2::xml_find_first(codes, "./Description"))
    )
  }
)

#' @title Fetch UNICEF SDMX data or structure
#' @description
#' Download UNICEF indicator data from the SDMX data warehouse.
#' Supports automatic paging, retrying on transient failure, memoisation, and tidy-up.
#' 
#' This function uses unified parameter names consistent with the Python package.
#'
#' @section Time Period Handling:
#' The UNICEF SDMX API returns TIME_PERIOD values in various formats (annual "2020" 
#' or monthly "2020-03"). This function automatically converts monthly periods to 
#' decimal years for consistent time-series analysis:
#' \itemize{
#'   \item "2020" becomes 2020.0 (integer year)
#'   \item "2020-01" becomes 2020.0833 (2020 + 1/12, January)
#'   \item "2020-06" becomes 2020.5000 (2020 + 6/12, June)
#'   \item "2020-11" becomes 2020.9167 (2020 + 11/12, November)
#' }
#' Formula: decimal_year = year + month/12
#'
#' @param indicator Character vector of indicator codes (e.g., "CME_MRY0T4").
#'   Previously called 'key'. Both parameter names are supported.
#' @param dataflow Character vector of dataflow IDs (e.g., "CME", "NUTRITION").
#'   Previously called 'flow'. Both parameter names are supported.
#' @param countries Character vector of ISO3 country codes (e.g., c("ALB", "USA")).
#'   If NULL (default), fetches all countries.
#' @param start_year Optional starting year (e.g., 2015).
#'   Previously called 'start_period'. Both parameter names are supported.
#' @param end_year Optional ending year (e.g., 2023).
#'   Previously called 'end_period'. Both parameter names are supported.
#' @param sex Sex disaggregation: "_T" (total, default), "F" (female), "M" (male).
#' @param tidy Logical; if TRUE (default), returns cleaned tibble with standardized column names.
#' @param country_names Logical; if TRUE (default), adds country name column.
#' @param max_retries Number of retry attempts on failure (default: 3).
#'   Previously called 'retry'. Both parameter names are supported.
#' @param cache Logical; if TRUE, memoises results.
#' @param page_size Integer rows per page (default: 100000).
#' @param detail "data" (default) or "structure" for metadata.
#' @param version Optional SDMX version; if NULL, auto-detected.
#' @param format Output format: "long" (default), "wide" (years as columns), 
#'   or "wide_indicators" (indicators as columns).
#' @param latest Logical; if TRUE, keep only the most recent non-missing value per country.
#'   The year may differ by country. Useful for cross-sectional analysis.
#' @param add_metadata Character vector of metadata to add: "region", "income_group", 
#'   "continent", "indicator_name", "indicator_category".
#' @param dropna Logical; if TRUE, remove rows with missing values.
#' @param simplify Logical; if TRUE, keep only essential columns.
#' @param mrv Integer; keep only the N most recent values per country (Most Recent Values).
#' @param raw Logical; if TRUE, return raw SDMX data without column standardization.
#'   Default is FALSE (clean, standardized output matching Python package).
#' @param ignore_duplicates Logical; if FALSE (default), raises an error when exact
#'   duplicate rows are found (all column values identical). Set to TRUE to allow 
#'   automatic removal of duplicates.
#' @param flow Deprecated. Use 'dataflow' instead.
#' @param key Deprecated. Use 'indicator' instead.
#' @param start_period Deprecated. Use 'start_year' instead.
#' @param end_period Deprecated. Use 'end_year' instead.
#' @param retry Deprecated. Use 'max_retries' instead.
#' @return Tibble with indicator data, or xml_document if detail="structure".
#'   The 'period' column contains decimal years (see Time Period Handling section).
#' 
#' @examples
#' \dontrun{
#' # Fetch under-5 mortality for specific countries (clean output)
#' df <- get_unicef(
#'   indicator = "CME_MRY0T4",
#'   countries = c("ALB", "USA", "BRA"),
#'   start_year = 2015,
#'   end_year = 2023
#' )
#' 
#' # Get raw SDMX data with all original columns
#' df_raw <- get_unicef(
#'   indicator = "CME_MRY0T4",
#'   countries = c("ALB", "USA"),
#'   raw = TRUE
#' )
#' 
#' # Get latest value per country (cross-sectional)
#' df <- get_unicef(
#'   indicator = "CME_MRY0T4",
#'   latest = TRUE
#' )
#' 
#' # Wide format with region metadata
#' df <- get_unicef(
#'   indicator = "CME_MRY0T4",
#'   format = "wide",
#'   add_metadata = c("region", "income_group")
#' )
#' 
#' # Multiple indicators merged automatically
#' df <- get_unicef(
#'   indicator = c("CME_MRY0T4", "NT_ANT_HAZ_NE2_MOD"),
#'   format = "wide_indicators",
#'   latest = TRUE
#' )
#' 
#' # Legacy syntax (still supported)
#' df <- get_unicef(flow = "CME", key = "CME_MRY0T4")
#' }
#' @export
get_unicef <- function(
    # New unified parameter names
    indicator     = NULL,
    dataflow      = NULL,
    countries     = NULL,
    start_year    = NULL,
    end_year      = NULL,
    sex           = "_T",
    tidy          = TRUE,
    country_names = TRUE,
    max_retries   = 3,
    cache         = FALSE,
    page_size     = 100000,
    detail        = c("data", "structure"),
    version       = NULL,
    # NEW: Post-production options
    format        = c("long", "wide", "wide_indicators"),
    latest        = FALSE,
    add_metadata  = NULL,
    dropna        = FALSE,
    simplify      = FALSE,
    mrv           = NULL,
    raw           = FALSE,
    ignore_duplicates = FALSE,
    # Legacy parameter names (deprecated but supported)
    flow          = NULL,
    key           = NULL,
    start_period  = NULL,
    end_period    = NULL,
    retry         = NULL
) {
  # Handle legacy parameter names with deprecation warnings
  if (!is.null(flow) && is.null(dataflow)) {
    dataflow <- flow
    # message("Note: 'flow' is deprecated. Use 'dataflow' instead.")
  }
  if (!is.null(key) && is.null(indicator)) {
    indicator <- key
    # message("Note: 'key' is deprecated. Use 'indicator' instead.")
  }
  if (!is.null(start_period) && is.null(start_year)) {
    start_year <- start_period
    # message("Note: 'start_period' is deprecated. Use 'start_year' instead.")
  }
  if (!is.null(end_period) && is.null(end_year)) {
    end_year <- end_period
    # message("Note: 'end_period' is deprecated. Use 'end_year' instead.")
  }
  if (!is.null(retry) && max_retries == 3) {
    max_retries <- retry
    # message("Note: 'retry' is deprecated. Use 'max_retries' instead.")
  }
  
  format <- match.arg(format)
  
  # Auto-detect dataflow from indicator code if not provided
  if (is.null(dataflow) && !is.null(indicator)) {
    # Use indicator registry to get dataflow
    # Source the registry if get_dataflow_for_indicator is not available
    if (!exists("get_dataflow_for_indicator", mode = "function")) {
      script_file <- sys.frame(1)$ofile
      script_dir <- if (is.null(script_file)) "." else dirname(script_file)
      registry_path <- file.path(script_dir, "indicator_registry.R")
      if (file.exists(registry_path)) {
        source(registry_path, local = FALSE)
      }
    }
    
    # Try to auto-detect dataflow
    first_indicator <- if (is.character(indicator)) indicator[1] else as.character(indicator)[1]
    
    if (exists("get_dataflow_for_indicator", mode = "function")) {
      dataflow <- get_dataflow_for_indicator(first_indicator)
      message(sprintf("Auto-detected dataflow '%s' for indicator '%s'", dataflow, first_indicator))
    } else {
      # Fallback: infer from indicator prefix
      # First check known overrides for problematic indicators
      indicator_overrides <- list(
        # Child Marriage - metadata says PT but data is in PT_CM
        "PT_F_20-24_MRD_U18_TND" = "PT_CM",
        "PT_F_20-24_MRD_U15" = "PT_CM",
        # FGM - metadata says PT but data is in PT_FGM
        "PT_F_15-49_FGM" = "PT_FGM",
        "PT_F_0-14_FGM" = "PT_FGM",
        "PT_F_15-19_FGM_TND" = "PT_FGM",
        "PT_F_15-49_FGM_TND" = "PT_FGM",
        "PT_F_15-49_FGM_ELIM" = "PT_FGM",
        # Education UIS SDG indicators
        "ED_CR_L1_UIS_MOD" = "EDUCATION_UIS_SDG",
        "ED_CR_L2_UIS_MOD" = "EDUCATION_UIS_SDG",
        "ED_CR_L3_UIS_MOD" = "EDUCATION_UIS_SDG",
        "ED_ROFST_L1_UIS_MOD" = "EDUCATION_UIS_SDG",
        "ED_ROFST_L2_UIS_MOD" = "EDUCATION_UIS_SDG",
        "ED_ROFST_L3_UIS_MOD" = "EDUCATION_UIS_SDG",
        "ED_ANAR_L02" = "EDUCATION_UIS_SDG",
        "ED_MAT_G23" = "EDUCATION_UIS_SDG",
        "ED_MAT_L1" = "EDUCATION_UIS_SDG",
        "ED_MAT_L2" = "EDUCATION_UIS_SDG",
        "ED_READ_G23" = "EDUCATION_UIS_SDG",
        "ED_READ_L1" = "EDUCATION_UIS_SDG",
        "ED_READ_L2" = "EDUCATION_UIS_SDG",
        # Child Poverty
        "PV_CHLD_DPRV-S-L1-HS" = "CHLD_PVTY"
      )
      
      if (first_indicator %in% names(indicator_overrides)) {
        dataflow <- indicator_overrides[[first_indicator]]
        message(sprintf("Using known dataflow override '%s' for indicator '%s'", dataflow, first_indicator))
      } else {
        parts <- strsplit(first_indicator, "_")[[1]]
        prefix <- parts[1]
      
        # Common prefix mappings
        prefix_map <- list(
          CME = "CME",
          NT = "NUTRITION",
          IM = "IMMUNISATION",
          ED = "EDUCATION",
          WS = "WASH_HOUSEHOLDS",
          HVA = "HIV_AIDS",
          MNCH = "MNCH",
          PT = "PT",
          ECD = "ECD",
          PV = "CHLD_PVTY"
        )
      
        if (prefix %in% names(prefix_map)) {
          dataflow <- prefix_map[[prefix]]
          message(sprintf("Inferred dataflow '%s' from indicator prefix '%s'", dataflow, prefix))
        } else {
          dataflow <- "GLOBAL_DATAFLOW"
          message(sprintf("Using GLOBAL_DATAFLOW for indicator '%s'", first_indicator))
        }
      }
    }
  }
  
  # Validate: at least dataflow or indicator must be specified
  if (is.null(dataflow)) {
    stop("Either 'indicator' or 'dataflow' must be specified. Use list_dataflows() to see available options.", call. = FALSE)
  }
  detail <- match.arg(detail)
  stopifnot(is.character(dataflow), length(dataflow) >= 1)
  
  validate_year <- function(x, name) {
    if (!is.null(x)) {
      x_chr <- as.character(x)
      if (length(x_chr) != 1 || !grepl("^\\d{4}$", x_chr))
        stop(sprintf("`%s` must be a single 4-digit year.", name), call. = FALSE)
      return(x_chr)
    }
    NULL
  }
  start_year_str <- validate_year(start_year, "start_year")
  end_year_str   <- validate_year(end_year,   "end_year")
  
  if (is.null(version) && detail == "data") flows_meta <- list_unicef_flows(retry = max_retries)
  
  fetch_flow <- function(fl) {
    ua <- httr::user_agent("get_unicef/1.0 (+https://github.com/jpazvd/get_unicef)")
    base <- "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"
    
    if (detail == "structure") {
      ver <- if (!is.null(version)) version else {
        idx <- match(fl, flows_meta$id)
        if (is.na(idx)) stop(sprintf("Dataflow '%s' not found.", fl), call. = FALSE)
        flows_meta$version[idx]
      }
      url <- sprintf("%s/structure/dataflow/UNICEF.%s?references=all&detail=full", base, fl)
      return(xml2::read_xml(fetch_sdmx(url, ua, max_retries)))
    }
    
    ver <- if (!is.null(version)) version else {
      idx <- match(fl, flows_meta$id)
      if (is.na(idx)) stop(sprintf("Dataflow '%s' not found.", fl), call. = FALSE)
      flows_meta$version[idx]
    }
    
    # Build data key: .INDICATOR. format for SDMX query
    indicator_str <- if (!is.null(indicator)) paste0(".", paste(indicator, collapse = "+"), ".") else "."
    
    # Build URL in format: data/UNICEF,DATAFLOW,VERSION/DATA_KEY
    rel_path <- sprintf("data/UNICEF,%s,%s/%s", fl, ver, indicator_str)
    full_url <- paste0(base, "/", rel_path, "?format=csv&labels=both")
    
    # Add date range parameters
    if (!is.null(start_year_str)) {
      full_url <- paste0(full_url, "&startPeriod=", start_year_str)
    }
    if (!is.null(end_year_str)) {
      full_url <- paste0(full_url, "&endPeriod=", end_year_str)
    }
    
    # paging
    pages <- list(); page <- 0L
    repeat {
      page_url <- paste0(full_url, "&startIndex=", page * page_size,
                         "&count=", page_size)
      df <- tryCatch(
        readr::read_csv(fetch_sdmx(page_url, ua, max_retries), show_col_types = FALSE),
        error = function(e) NULL
      )
      if (is.null(df) || nrow(df) == 0) break
      pages[[length(pages) + 1L]] <- df
      if (nrow(df) < page_size) break
      page <- page + 1L; Sys.sleep(0.2)
    }
    df_all <- dplyr::bind_rows(pages)
    
    # Filter by countries if specified
    if (!is.null(countries) && nrow(df_all) > 0) {
      if ("REF_AREA" %in% names(df_all)) {
        df_all <- df_all %>% dplyr::filter(REF_AREA %in% countries)
      }
    }
    
    # =========================================================================
    # Filter to totals by default (sex, age, wealth_quintile)
    # This provides clean aggregated data; disaggregations available on request
    # =========================================================================
    
    available_disaggregations <- c()
    
    # Filter by sex (default is "_T" for total)
    if (nrow(df_all) > 0 && "SEX" %in% names(df_all)) {
      sex_values <- unique(na.omit(df_all$SEX))
      if (length(sex_values) > 1 || (length(sex_values) == 1 && sex_values[1] != "_T")) {
        available_disaggregations <- c(available_disaggregations, 
                                       paste0("sex: ", paste(sex_values, collapse = ", ")))
      }
      if (!is.null(sex)) {
        df_all <- df_all %>% dplyr::filter(SEX == sex)
      }
    }
    
    # Filter by age (default: keep only total age groups)
    if (nrow(df_all) > 0 && "AGE" %in% names(df_all)) {
      age_values <- unique(na.omit(df_all$AGE))
      if (length(age_values) > 1) {
        available_disaggregations <- c(available_disaggregations,
                                       paste0("age: ", paste(age_values, collapse = ", ")))
        # Keep only total age groups
        total_ages <- c("_T", "Y0T4", "Y0T14", "Y0T17", "Y15T49", "ALLAGE")
        age_totals <- intersect(total_ages, age_values)
        if (length(age_totals) > 0) {
          df_all <- df_all %>% dplyr::filter(AGE %in% age_totals)
        }
      }
    }
    
    # Filter by wealth quintile (default: total)
    if (nrow(df_all) > 0 && "WEALTH_QUINTILE" %in% names(df_all)) {
      wq_values <- unique(na.omit(df_all$WEALTH_QUINTILE))
      if (length(wq_values) > 1 || (length(wq_values) == 1 && wq_values[1] != "_T")) {
        available_disaggregations <- c(available_disaggregations,
                                       paste0("wealth_quintile: ", paste(wq_values, collapse = ", ")))
      }
      if ("_T" %in% wq_values) {
        df_all <- df_all %>% dplyr::filter(WEALTH_QUINTILE == "_T")
      }
    }
    
    # Filter by residence (default: total)
    if (nrow(df_all) > 0 && "RESIDENCE" %in% names(df_all)) {
      res_values <- unique(na.omit(df_all$RESIDENCE))
      if (length(res_values) > 1 || (length(res_values) == 1 && res_values[1] != "_T")) {
        available_disaggregations <- c(available_disaggregations,
                                       paste0("residence: ", paste(res_values, collapse = ", ")))
      }
      if ("_T" %in% res_values) {
        df_all <- df_all %>% dplyr::filter(RESIDENCE == "_T")
      }
    }
    
    # Filter by maternal education level (default: total)
    if (nrow(df_all) > 0 && "MATERNAL_EDU_LVL" %in% names(df_all)) {
      edu_values <- unique(na.omit(df_all$MATERNAL_EDU_LVL))
      if (length(edu_values) > 1 || (length(edu_values) == 1 && edu_values[1] != "_T")) {
        available_disaggregations <- c(available_disaggregations,
                                       paste0("maternal_edu_lvl: ", paste(edu_values, collapse = ", ")))
      }
      if ("_T" %in% edu_values) {
        df_all <- df_all %>% dplyr::filter(MATERNAL_EDU_LVL == "_T")
      }
    }
    
    # Log available disaggregations
    if (length(available_disaggregations) > 0 && !raw) {
      message(sprintf("Note: Disaggregated data available for %s: %s. Use raw=TRUE to access.",
                      if(!is.null(indicator)) indicator[1] else fl,
                      paste(available_disaggregations, collapse = "; ")))
    }
    
    # If raw=TRUE, return the data with minimal processing (just rename core columns)
    if (raw && nrow(df_all) > 0) {
      # Minimal renaming for usability, but keep all original columns
      df_all <- df_all %>%
        dplyr::rename(
          iso3      = REF_AREA,
          indicator = INDICATOR,
          period    = TIME_PERIOD,
          value     = OBS_VALUE
        ) %>%
        dplyr::mutate(
          period = as.integer(period),
          value = as.numeric(value)
        ) %>%
        dplyr::select(iso3, indicator, period, value, dplyr::everything())
      
      if (country_names) {
        df_all <- df_all %>%
          dplyr::left_join(
            countrycode::codelist %>% dplyr::select(iso3 = iso3c, country = country.name.en),
            by = "iso3"
          ) %>% 
          dplyr::select(iso3, country, dplyr::everything())
      }
      return(df_all)
    }
    
    if (tidy && nrow(df_all) > 0) {
      # Standardize column names to match Python output
      # Columns: indicator, iso3, country, period, value, unit, unit_name, sex, 
      #          age, wealth_quintile, residence, maternal_edu_lvl,
      #          lower_bound, upper_bound, obs_status, data_source
      
      # Helper function to convert TIME_PERIOD to decimal year
      # e.g., "2006-01" -> 2006 + 1/12 = 2006.0833, "2006-11" -> 2006 + 11/12 = 2006.9167
      convert_period_to_decimal <- function(val) {
        if (is.na(val)) return(NA_real_)
        val_str <- as.character(val)
        # Check for YYYY-MM format
        if (grepl("^\\d{4}-\\d{2}", val_str)) {
          parts <- strsplit(val_str, "-")[[1]]
          year <- as.numeric(parts[1])
          month <- as.numeric(parts[2])
          return(year + month / 12)
        }
        # Try direct numeric conversion for YYYY format
        return(as.numeric(val_str))
      }
      
      # Build clean output with consistent column names
      # Including all disaggregation columns for transparency
      df_clean <- df_all %>%
        dplyr::transmute(
          indicator = INDICATOR,
          iso3 = REF_AREA,
          period = sapply(TIME_PERIOD, convert_period_to_decimal),
          value = as.numeric(OBS_VALUE),
          unit = if ("UNIT_MEASURE" %in% names(.)) UNIT_MEASURE else NA_character_,
          unit_name = if ("Unit of measure" %in% names(.)) `Unit of measure` else NA_character_,
          sex = if ("SEX" %in% names(.)) SEX else NA_character_,
          age = if ("AGE" %in% names(.)) AGE else NA_character_,
          wealth_quintile = if ("WEALTH_QUINTILE" %in% names(.)) WEALTH_QUINTILE else NA_character_,
          residence = if ("RESIDENCE" %in% names(.)) RESIDENCE else NA_character_,
          maternal_edu_lvl = if ("MATERNAL_EDU_LVL" %in% names(.)) MATERNAL_EDU_LVL else NA_character_,
          lower_bound = if ("LOWER_BOUND" %in% names(.)) as.numeric(LOWER_BOUND) else NA_real_,
          upper_bound = if ("UPPER_BOUND" %in% names(.)) as.numeric(UPPER_BOUND) else NA_real_,
          obs_status = if ("OBS_STATUS" %in% names(.)) OBS_STATUS else NA_character_,
          data_source = if ("DATA_SOURCE" %in% names(.)) DATA_SOURCE else NA_character_
        )
      
      # Add country name if requested
      if (country_names) {
        df_clean <- df_clean %>%
          dplyr::left_join(
            countrycode::codelist %>% dplyr::select(iso3 = iso3c, country = country.name.en),
            by = "iso3"
          ) %>%
          dplyr::select(indicator, iso3, country, dplyr::everything())
      }
      
      # Check for exact duplicates - rows where ALL column values are identical
      n_before <- nrow(df_clean)
      df_clean_deduped <- dplyr::distinct(df_clean)
      n_duplicates <- n_before - nrow(df_clean_deduped)
      
      if (n_duplicates > 0) {
        if (!ignore_duplicates) {
          stop(sprintf(
            "Found %d exact duplicate rows (all values identical). Set ignore_duplicates=TRUE to automatically remove duplicates.",
            n_duplicates
          ), call. = FALSE)
        } else {
          df_clean <- df_clean_deduped
          warning(sprintf(
            "Removed %d exact duplicate rows (all values identical).",
            n_duplicates
          ), call. = FALSE)
        }
      } else {
        df_clean <- df_clean_deduped
      }
      
      df_all <- df_clean
    }
    df_all
  }
  
  # ===========================================================================
  # DATAFLOW FALLBACK LOGIC
  # ===========================================================================
  # Alternative dataflows to try when auto-detected dataflow fails with 404
  dataflow_alternatives <- list(
    ED = c("EDUCATION_UIS_SDG", "EDUCATION", "GLOBAL_DATAFLOW"),
    PT = c("PT", "PT_CM", "PT_FGM", "PT_CHLD", "GLOBAL_DATAFLOW"),
    PV = c("CHLD_PVTY", "GLOBAL_DATAFLOW"),
    NT = c("NUTRITION", "GLOBAL_DATAFLOW")
  )
  
  # Wrapper function that tries alternative dataflows on failure
  fetch_with_fallback <- function(fl) {
    # Try the primary dataflow first
    result <- tryCatch(
      fetch_flow(fl),
      error = function(e) {
        # Check if it's a 404-like error
        if (grepl("404|not found|Request failed", e$message, ignore.case = TRUE)) {
          NULL
        } else {
          stop(e)  # Re-throw non-404 errors
        }
      }
    )
    
    # If result is empty or NULL, try alternatives
    if (is.null(result) || nrow(result) == 0) {
      # Get indicator prefix
      first_ind <- if (!is.null(indicator)) indicator[1] else NULL
      if (!is.null(first_ind)) {
        prefix <- strsplit(first_ind, "_")[[1]][1]
        
        if (prefix %in% names(dataflow_alternatives)) {
          alternatives <- dataflow_alternatives[[prefix]]
          alternatives <- alternatives[alternatives != fl]  # Remove already tried
          
          for (alt_flow in alternatives) {
            message(sprintf("Trying alternative dataflow '%s' for indicator '%s'...", 
                            alt_flow, first_ind))
            result <- tryCatch(
              fetch_flow(alt_flow),
              error = function(e) NULL
            )
            if (!is.null(result) && nrow(result) > 0) {
              message(sprintf("Success: Found data in dataflow '%s'", alt_flow))
              return(result)
            }
          }
        }
      }
    }
    
    # Return whatever we got (may be empty)
    if (is.null(result)) return(data.frame())
    return(result)
  }
  
  executor <- if (cache) memoise::memoise(fetch_with_fallback) else fetch_with_fallback
  result <- purrr::map(dataflow, executor)
  
  # Combine results if multiple dataflows
  if (length(result) == 1) {
    result <- result[[1]]
  } else {
    result <- dplyr::bind_rows(result)
  }
  
  # Return early if empty or structure request
  if (detail == "structure" || is.null(result) || nrow(result) == 0) {
    return(result)
  }
  
  # ==========================================================================
  # POST-PRODUCTION PROCESSING
  # ==========================================================================
  
  # 1. Add metadata columns
  if (!is.null(add_metadata) && "iso3" %in% names(result)) {
    result <- add_country_metadata(result, add_metadata)
    result <- add_indicator_metadata(result, add_metadata)
  }
  
  # 2. Drop NA values (period and value must be non-null for valid observations)
  if (dropna && "value" %in% names(result)) {
    result <- result %>% dplyr::filter(!is.na(value))
  }
  if ("period" %in% names(result)) {
    n_before <- nrow(result)
    result <- result %>% dplyr::filter(!is.na(period))
    n_removed <- n_before - nrow(result)
    if (n_removed > 0) {
      message(sprintf("Removed %d observations with missing time period", n_removed))
    }
  }
  
  # 3. Most Recent Values (MRV)
  if (!is.null(mrv) && mrv > 0 && "iso3" %in% names(result) && "period" %in% names(result)) {
    result <- apply_mrv(result, mrv)
  }
  
  # 4. Latest value per country
  if (latest && "iso3" %in% names(result) && "period" %in% names(result)) {
    result <- apply_latest(result)
  }
  
  # 5. Format transformation (long/wide)
  if (format != "long" && "iso3" %in% names(result)) {
    result <- apply_format(result, format)
  }
  
  # 6. Simplify columns
  if (simplify) {
    result <- simplify_columns(result, format)
  }
  
  result
}


#' Add country-level metadata columns
#' @keywords internal
add_country_metadata <- function(df, metadata_list) {
  if ("region" %in% metadata_list) {
    region_map <- get_country_regions()
    df <- df %>% dplyr::mutate(region = region_map[iso3])
  }
  
  if ("income_group" %in% metadata_list) {
    income_map <- get_income_groups()
    df <- df %>% dplyr::mutate(income_group = income_map[iso3])
  }
  
  if ("continent" %in% metadata_list) {
    continent_map <- get_continents()
    df <- df %>% dplyr::mutate(continent = continent_map[iso3])
  }
  
  df
}


#' Add indicator-level metadata columns
#' @keywords internal
add_indicator_metadata <- function(df, metadata_list) {
  if (!"indicator" %in% names(df)) return(df)
  
  if ("indicator_name" %in% metadata_list || "indicator_category" %in% metadata_list) {
    unique_inds <- unique(df$indicator)
    
    for (ind in unique_inds) {
      info <- tryCatch(get_indicator_info(ind), error = function(e) NULL)
      if (!is.null(info)) {
        if ("indicator_name" %in% metadata_list) {
          df <- df %>% dplyr::mutate(
            indicator_name = dplyr::if_else(indicator == ind, info$name, indicator_name)
          )
        }
        if ("indicator_category" %in% metadata_list) {
          df <- df %>% dplyr::mutate(
            indicator_category = dplyr::if_else(indicator == ind, info$category, indicator_category)
          )
        }
      }
    }
  }
  
  df
}


#' Apply Most Recent Values filter
#' @keywords internal
apply_mrv <- function(df, n) {
  if ("indicator" %in% names(df)) {
    df %>%
      dplyr::arrange(iso3, indicator, dplyr::desc(period)) %>%
      dplyr::group_by(iso3, indicator) %>%
      dplyr::slice_head(n = n) %>%
      dplyr::ungroup()
  } else {
    df %>%
      dplyr::arrange(iso3, dplyr::desc(period)) %>%
      dplyr::group_by(iso3) %>%
      dplyr::slice_head(n = n) %>%
      dplyr::ungroup()
  }
}


#' Apply latest value filter
#' @keywords internal
apply_latest <- function(df) {
  if ("value" %in% names(df)) {
    df <- df %>% dplyr::filter(!is.na(value))
  }
  
  if ("indicator" %in% names(df)) {
    df %>%
      dplyr::group_by(iso3, indicator) %>%
      dplyr::filter(period == max(period, na.rm = TRUE)) %>%
      dplyr::ungroup()
  } else {
    df %>%
      dplyr::group_by(iso3) %>%
      dplyr::filter(period == max(period, na.rm = TRUE)) %>%
      dplyr::ungroup()
  }
}


#' Apply format transformation
#' @keywords internal
apply_format <- function(df, format) {
  if (format == "wide") {
    # Countries as rows, years as columns
    n_indicators <- dplyr::n_distinct(df$indicator)
    if (n_indicators > 1) {
      message("Warning: 'wide' format with multiple indicators may produce complex output.")
      message("         Consider using 'wide_indicators' format instead.")
    }
    
    # Identify columns to keep as index
    id_cols <- c("iso3")
    if ("country" %in% names(df)) id_cols <- c(id_cols, "country")
    for (col in c("region", "income_group", "continent")) {
      if (col %in% names(df)) id_cols <- c(id_cols, col)
    }
    if (n_indicators > 1 && "indicator" %in% names(df)) {
      id_cols <- c(id_cols, "indicator")
    }
    
    df %>%
      tidyr::pivot_wider(
        id_cols = dplyr::all_of(id_cols),
        names_from = period,
        values_from = value,
        names_prefix = "y"
      )
    
  } else if (format == "wide_indicators") {
    # Years as rows, indicators as columns
    n_indicators <- dplyr::n_distinct(df$indicator)
    if (n_indicators == 1) {
      message("Warning: 'wide_indicators' format is designed for multiple indicators.")
      return(df)
    }
    
    # Identify columns to keep as index
    id_cols <- c("iso3", "period")
    if ("country" %in% names(df)) id_cols <- c(id_cols[1], "country", id_cols[2])
    for (col in c("region", "income_group", "continent")) {
      if (col %in% names(df)) id_cols <- c(id_cols, col)
    }
    
    df %>%
      tidyr::pivot_wider(
        id_cols = dplyr::all_of(id_cols),
        names_from = indicator,
        values_from = value
      )
    
  } else {
    df
  }
}


#' Simplify columns to essentials
#' @keywords internal
simplify_columns <- function(df, format) {
  if (format == "long") {
    essential <- c("iso3", "country", "indicator", "period", "value")
    metadata_cols <- c("region", "income_group", "continent", "indicator_name")
    available <- intersect(c(essential, metadata_cols), names(df))
    df %>% dplyr::select(dplyr::all_of(available))
  } else {
    df
  }
}


#' Get ISO3 to UNICEF region mapping
#' @keywords internal
get_country_regions <- function() {
  c(
    # East Asia and Pacific
    'AUS' = 'East Asia and Pacific', 'BRN' = 'East Asia and Pacific', 'KHM' = 'East Asia and Pacific',
    'CHN' = 'East Asia and Pacific', 'PRK' = 'East Asia and Pacific', 'FJI' = 'East Asia and Pacific',
    'IDN' = 'East Asia and Pacific', 'JPN' = 'East Asia and Pacific', 'KIR' = 'East Asia and Pacific',
    'LAO' = 'East Asia and Pacific', 'MYS' = 'East Asia and Pacific', 'MHL' = 'East Asia and Pacific',
    'FSM' = 'East Asia and Pacific', 'MNG' = 'East Asia and Pacific', 'MMR' = 'East Asia and Pacific',
    'NRU' = 'East Asia and Pacific', 'NZL' = 'East Asia and Pacific', 'PLW' = 'East Asia and Pacific',
    'PNG' = 'East Asia and Pacific', 'PHL' = 'East Asia and Pacific', 'WSM' = 'East Asia and Pacific',
    'SGP' = 'East Asia and Pacific', 'SLB' = 'East Asia and Pacific', 'KOR' = 'East Asia and Pacific',
    'THA' = 'East Asia and Pacific', 'TLS' = 'East Asia and Pacific', 'TON' = 'East Asia and Pacific',
    'TUV' = 'East Asia and Pacific', 'VUT' = 'East Asia and Pacific', 'VNM' = 'East Asia and Pacific',
    # Europe and Central Asia
    'ALB' = 'Europe and Central Asia', 'ARM' = 'Europe and Central Asia', 'AUT' = 'Europe and Central Asia',
    'AZE' = 'Europe and Central Asia', 'BLR' = 'Europe and Central Asia', 'BEL' = 'Europe and Central Asia',
    'BIH' = 'Europe and Central Asia', 'BGR' = 'Europe and Central Asia', 'HRV' = 'Europe and Central Asia',
    'CYP' = 'Europe and Central Asia', 'CZE' = 'Europe and Central Asia', 'DNK' = 'Europe and Central Asia',
    'EST' = 'Europe and Central Asia', 'FIN' = 'Europe and Central Asia', 'FRA' = 'Europe and Central Asia',
    'GEO' = 'Europe and Central Asia', 'DEU' = 'Europe and Central Asia', 'GRC' = 'Europe and Central Asia',
    'HUN' = 'Europe and Central Asia', 'ISL' = 'Europe and Central Asia', 'IRL' = 'Europe and Central Asia',
    'ITA' = 'Europe and Central Asia', 'KAZ' = 'Europe and Central Asia', 'KGZ' = 'Europe and Central Asia',
    'LVA' = 'Europe and Central Asia', 'LTU' = 'Europe and Central Asia', 'LUX' = 'Europe and Central Asia',
    'MKD' = 'Europe and Central Asia', 'MLT' = 'Europe and Central Asia', 'MDA' = 'Europe and Central Asia',
    'MNE' = 'Europe and Central Asia', 'NLD' = 'Europe and Central Asia', 'NOR' = 'Europe and Central Asia',
    'POL' = 'Europe and Central Asia', 'PRT' = 'Europe and Central Asia', 'ROU' = 'Europe and Central Asia',
    'RUS' = 'Europe and Central Asia', 'SRB' = 'Europe and Central Asia', 'SVK' = 'Europe and Central Asia',
    'SVN' = 'Europe and Central Asia', 'ESP' = 'Europe and Central Asia', 'SWE' = 'Europe and Central Asia',
    'CHE' = 'Europe and Central Asia', 'TJK' = 'Europe and Central Asia', 'TUR' = 'Europe and Central Asia',
    'TKM' = 'Europe and Central Asia', 'UKR' = 'Europe and Central Asia', 'GBR' = 'Europe and Central Asia',
    'UZB' = 'Europe and Central Asia',
    # Latin America and Caribbean
    'ATG' = 'Latin America and Caribbean', 'ARG' = 'Latin America and Caribbean', 'BHS' = 'Latin America and Caribbean',
    'BRB' = 'Latin America and Caribbean', 'BLZ' = 'Latin America and Caribbean', 'BOL' = 'Latin America and Caribbean',
    'BRA' = 'Latin America and Caribbean', 'CHL' = 'Latin America and Caribbean', 'COL' = 'Latin America and Caribbean',
    'CRI' = 'Latin America and Caribbean', 'CUB' = 'Latin America and Caribbean', 'DMA' = 'Latin America and Caribbean',
    'DOM' = 'Latin America and Caribbean', 'ECU' = 'Latin America and Caribbean', 'SLV' = 'Latin America and Caribbean',
    'GRD' = 'Latin America and Caribbean', 'GTM' = 'Latin America and Caribbean', 'GUY' = 'Latin America and Caribbean',
    'HTI' = 'Latin America and Caribbean', 'HND' = 'Latin America and Caribbean', 'JAM' = 'Latin America and Caribbean',
    'MEX' = 'Latin America and Caribbean', 'NIC' = 'Latin America and Caribbean', 'PAN' = 'Latin America and Caribbean',
    'PRY' = 'Latin America and Caribbean', 'PER' = 'Latin America and Caribbean', 'KNA' = 'Latin America and Caribbean',
    'LCA' = 'Latin America and Caribbean', 'VCT' = 'Latin America and Caribbean', 'SUR' = 'Latin America and Caribbean',
    'TTO' = 'Latin America and Caribbean', 'URY' = 'Latin America and Caribbean', 'VEN' = 'Latin America and Caribbean',
    # Middle East and North Africa
    'DZA' = 'Middle East and North Africa', 'BHR' = 'Middle East and North Africa', 'DJI' = 'Middle East and North Africa',
    'EGY' = 'Middle East and North Africa', 'IRN' = 'Middle East and North Africa', 'IRQ' = 'Middle East and North Africa',
    'ISR' = 'Middle East and North Africa', 'JOR' = 'Middle East and North Africa', 'KWT' = 'Middle East and North Africa',
    'LBN' = 'Middle East and North Africa', 'LBY' = 'Middle East and North Africa', 'MAR' = 'Middle East and North Africa',
    'OMN' = 'Middle East and North Africa', 'QAT' = 'Middle East and North Africa', 'SAU' = 'Middle East and North Africa',
    'SDN' = 'Middle East and North Africa', 'SYR' = 'Middle East and North Africa', 'TUN' = 'Middle East and North Africa',
    'ARE' = 'Middle East and North Africa', 'YEM' = 'Middle East and North Africa', 'PSE' = 'Middle East and North Africa',
    # North America
    'CAN' = 'North America', 'USA' = 'North America',
    # South Asia
    'AFG' = 'South Asia', 'BGD' = 'South Asia', 'BTN' = 'South Asia', 'IND' = 'South Asia',
    'MDV' = 'South Asia', 'NPL' = 'South Asia', 'PAK' = 'South Asia', 'LKA' = 'South Asia',
    # Sub-Saharan Africa
    'AGO' = 'Sub-Saharan Africa', 'BEN' = 'Sub-Saharan Africa', 'BWA' = 'Sub-Saharan Africa',
    'BFA' = 'Sub-Saharan Africa', 'BDI' = 'Sub-Saharan Africa', 'CPV' = 'Sub-Saharan Africa',
    'CMR' = 'Sub-Saharan Africa', 'CAF' = 'Sub-Saharan Africa', 'TCD' = 'Sub-Saharan Africa',
    'COM' = 'Sub-Saharan Africa', 'COG' = 'Sub-Saharan Africa', 'COD' = 'Sub-Saharan Africa',
    'CIV' = 'Sub-Saharan Africa', 'GNQ' = 'Sub-Saharan Africa', 'ERI' = 'Sub-Saharan Africa',
    'SWZ' = 'Sub-Saharan Africa', 'ETH' = 'Sub-Saharan Africa', 'GAB' = 'Sub-Saharan Africa',
    'GMB' = 'Sub-Saharan Africa', 'GHA' = 'Sub-Saharan Africa', 'GIN' = 'Sub-Saharan Africa',
    'GNB' = 'Sub-Saharan Africa', 'KEN' = 'Sub-Saharan Africa', 'LSO' = 'Sub-Saharan Africa',
    'LBR' = 'Sub-Saharan Africa', 'MDG' = 'Sub-Saharan Africa', 'MWI' = 'Sub-Saharan Africa',
    'MLI' = 'Sub-Saharan Africa', 'MRT' = 'Sub-Saharan Africa', 'MUS' = 'Sub-Saharan Africa',
    'MOZ' = 'Sub-Saharan Africa', 'NAM' = 'Sub-Saharan Africa', 'NER' = 'Sub-Saharan Africa',
    'NGA' = 'Sub-Saharan Africa', 'RWA' = 'Sub-Saharan Africa', 'STP' = 'Sub-Saharan Africa',
    'SEN' = 'Sub-Saharan Africa', 'SYC' = 'Sub-Saharan Africa', 'SLE' = 'Sub-Saharan Africa',
    'SOM' = 'Sub-Saharan Africa', 'ZAF' = 'Sub-Saharan Africa', 'SSD' = 'Sub-Saharan Africa',
    'TZA' = 'Sub-Saharan Africa', 'TGO' = 'Sub-Saharan Africa', 'UGA' = 'Sub-Saharan Africa',
    'ZMB' = 'Sub-Saharan Africa', 'ZWE' = 'Sub-Saharan Africa'
  )
}


#' Get ISO3 to World Bank income group mapping
#' @keywords internal
get_income_groups <- function() {
  c(
    # High income
    'AUS' = 'High income', 'AUT' = 'High income', 'BEL' = 'High income', 'CAN' = 'High income',
    'CHE' = 'High income', 'CHL' = 'High income', 'CZE' = 'High income', 'DEU' = 'High income',
    'DNK' = 'High income', 'ESP' = 'High income', 'EST' = 'High income', 'FIN' = 'High income',
    'FRA' = 'High income', 'GBR' = 'High income', 'GRC' = 'High income', 'HUN' = 'High income',
    'IRL' = 'High income', 'ISL' = 'High income', 'ISR' = 'High income', 'ITA' = 'High income',
    'JPN' = 'High income', 'KOR' = 'High income', 'LTU' = 'High income', 'LUX' = 'High income',
    'LVA' = 'High income', 'NLD' = 'High income', 'NOR' = 'High income', 'NZL' = 'High income',
    'POL' = 'High income', 'PRT' = 'High income', 'SAU' = 'High income', 'SGP' = 'High income',
    'SVK' = 'High income', 'SVN' = 'High income', 'SWE' = 'High income', 'USA' = 'High income',
    'URY' = 'High income', 'ARE' = 'High income', 'BHR' = 'High income', 'KWT' = 'High income',
    'OMN' = 'High income', 'QAT' = 'High income', 'HRV' = 'High income', 'CYP' = 'High income',
    'MLT' = 'High income', 'BRN' = 'High income', 'PAN' = 'High income', 'TTO' = 'High income',
    'BHS' = 'High income', 'BRB' = 'High income', 'ATG' = 'High income', 'KNA' = 'High income',
    'SYC' = 'High income', 'PLW' = 'High income', 'NRU' = 'High income',
    # Upper middle income
    'ARG' = 'Upper middle income', 'BGR' = 'Upper middle income', 'BRA' = 'Upper middle income',
    'CHN' = 'Upper middle income', 'COL' = 'Upper middle income', 'CRI' = 'Upper middle income',
    'DOM' = 'Upper middle income', 'ECU' = 'Upper middle income', 'GAB' = 'Upper middle income',
    'GNQ' = 'Upper middle income', 'GTM' = 'Upper middle income', 'IRN' = 'Upper middle income',
    'IRQ' = 'Upper middle income', 'JAM' = 'Upper middle income', 'JOR' = 'Upper middle income',
    'KAZ' = 'Upper middle income', 'LBN' = 'Upper middle income', 'LBY' = 'Upper middle income',
    'MEX' = 'Upper middle income', 'MKD' = 'Upper middle income', 'MNE' = 'Upper middle income',
    'MUS' = 'Upper middle income', 'MYS' = 'Upper middle income', 'NAM' = 'Upper middle income',
    'PER' = 'Upper middle income', 'ROU' = 'Upper middle income', 'RUS' = 'Upper middle income',
    'SRB' = 'Upper middle income', 'THA' = 'Upper middle income', 'TUR' = 'Upper middle income',
    'TKM' = 'Upper middle income', 'VEN' = 'Upper middle income', 'ZAF' = 'Upper middle income',
    'ALB' = 'Upper middle income', 'ARM' = 'Upper middle income', 'AZE' = 'Upper middle income',
    'BIH' = 'Upper middle income', 'BWA' = 'Upper middle income', 'CUB' = 'Upper middle income',
    'DMA' = 'Upper middle income', 'FJI' = 'Upper middle income', 'GEO' = 'Upper middle income',
    'GRD' = 'Upper middle income', 'GUY' = 'Upper middle income', 'LCA' = 'Upper middle income',
    'MDV' = 'Upper middle income', 'MHL' = 'Upper middle income', 'PRY' = 'Upper middle income',
    'SUR' = 'Upper middle income', 'TON' = 'Upper middle income', 'TUV' = 'Upper middle income',
    'VCT' = 'Upper middle income',
    # Lower middle income
    'AGO' = 'Lower middle income', 'BEN' = 'Lower middle income', 'BGD' = 'Lower middle income',
    'BLZ' = 'Lower middle income', 'BOL' = 'Lower middle income', 'BTN' = 'Lower middle income',
    'CIV' = 'Lower middle income', 'CMR' = 'Lower middle income', 'COG' = 'Lower middle income',
    'COM' = 'Lower middle income', 'CPV' = 'Lower middle income', 'DJI' = 'Lower middle income',
    'DZA' = 'Lower middle income', 'EGY' = 'Lower middle income', 'GHA' = 'Lower middle income',
    'HND' = 'Lower middle income', 'HTI' = 'Lower middle income', 'IDN' = 'Lower middle income',
    'IND' = 'Lower middle income', 'KEN' = 'Lower middle income', 'KGZ' = 'Lower middle income',
    'KHM' = 'Lower middle income', 'KIR' = 'Lower middle income', 'LAO' = 'Lower middle income',
    'LKA' = 'Lower middle income', 'LSO' = 'Lower middle income', 'MAR' = 'Lower middle income',
    'MDA' = 'Lower middle income', 'MMR' = 'Lower middle income', 'MNG' = 'Lower middle income',
    'MRT' = 'Lower middle income', 'NGA' = 'Lower middle income', 'NIC' = 'Lower middle income',
    'NPL' = 'Lower middle income', 'PAK' = 'Lower middle income', 'PHL' = 'Lower middle income',
    'PNG' = 'Lower middle income', 'PSE' = 'Lower middle income', 'SEN' = 'Lower middle income',
    'SLB' = 'Lower middle income', 'SLV' = 'Lower middle income', 'STP' = 'Lower middle income',
    'SWZ' = 'Lower middle income', 'TJK' = 'Lower middle income', 'TLS' = 'Lower middle income',
    'TUN' = 'Lower middle income', 'TZA' = 'Lower middle income', 'UKR' = 'Lower middle income',
    'UZB' = 'Lower middle income', 'VNM' = 'Lower middle income', 'VUT' = 'Lower middle income',
    'WSM' = 'Lower middle income', 'ZMB' = 'Lower middle income', 'ZWE' = 'Lower middle income',
    # Low income
    'AFG' = 'Low income', 'BDI' = 'Low income', 'BFA' = 'Low income', 'CAF' = 'Low income',
    'COD' = 'Low income', 'ERI' = 'Low income', 'ETH' = 'Low income', 'GMB' = 'Low income',
    'GIN' = 'Low income', 'GNB' = 'Low income', 'LBR' = 'Low income', 'MDG' = 'Low income',
    'MLI' = 'Low income', 'MOZ' = 'Low income', 'MWI' = 'Low income', 'NER' = 'Low income',
    'PRK' = 'Low income', 'RWA' = 'Low income', 'SDN' = 'Low income', 'SLE' = 'Low income',
    'SOM' = 'Low income', 'SSD' = 'Low income', 'SYR' = 'Low income', 'TCD' = 'Low income',
    'TGO' = 'Low income', 'UGA' = 'Low income', 'YEM' = 'Low income'
  )
}


#' Get ISO3 to continent mapping
#' @keywords internal
get_continents <- function() {
  c(
    # Africa
    'DZA' = 'Africa', 'AGO' = 'Africa', 'BEN' = 'Africa', 'BWA' = 'Africa', 'BFA' = 'Africa',
    'BDI' = 'Africa', 'CPV' = 'Africa', 'CMR' = 'Africa', 'CAF' = 'Africa', 'TCD' = 'Africa',
    'COM' = 'Africa', 'COG' = 'Africa', 'COD' = 'Africa', 'CIV' = 'Africa', 'DJI' = 'Africa',
    'EGY' = 'Africa', 'GNQ' = 'Africa', 'ERI' = 'Africa', 'SWZ' = 'Africa', 'ETH' = 'Africa',
    'GAB' = 'Africa', 'GMB' = 'Africa', 'GHA' = 'Africa', 'GIN' = 'Africa', 'GNB' = 'Africa',
    'KEN' = 'Africa', 'LSO' = 'Africa', 'LBR' = 'Africa', 'LBY' = 'Africa', 'MDG' = 'Africa',
    'MWI' = 'Africa', 'MLI' = 'Africa', 'MRT' = 'Africa', 'MUS' = 'Africa', 'MAR' = 'Africa',
    'MOZ' = 'Africa', 'NAM' = 'Africa', 'NER' = 'Africa', 'NGA' = 'Africa', 'RWA' = 'Africa',
    'STP' = 'Africa', 'SEN' = 'Africa', 'SYC' = 'Africa', 'SLE' = 'Africa', 'SOM' = 'Africa',
    'ZAF' = 'Africa', 'SSD' = 'Africa', 'SDN' = 'Africa', 'TZA' = 'Africa', 'TGO' = 'Africa',
    'TUN' = 'Africa', 'UGA' = 'Africa', 'ZMB' = 'Africa', 'ZWE' = 'Africa',
    # Asia
    'AFG' = 'Asia', 'ARM' = 'Asia', 'AZE' = 'Asia', 'BHR' = 'Asia', 'BGD' = 'Asia',
    'BTN' = 'Asia', 'BRN' = 'Asia', 'KHM' = 'Asia', 'CHN' = 'Asia', 'CYP' = 'Asia',
    'GEO' = 'Asia', 'IND' = 'Asia', 'IDN' = 'Asia', 'IRN' = 'Asia', 'IRQ' = 'Asia',
    'ISR' = 'Asia', 'JPN' = 'Asia', 'JOR' = 'Asia', 'KAZ' = 'Asia', 'KWT' = 'Asia',
    'KGZ' = 'Asia', 'LAO' = 'Asia', 'LBN' = 'Asia', 'MYS' = 'Asia', 'MDV' = 'Asia',
    'MNG' = 'Asia', 'MMR' = 'Asia', 'NPL' = 'Asia', 'PRK' = 'Asia', 'OMN' = 'Asia',
    'PAK' = 'Asia', 'PSE' = 'Asia', 'PHL' = 'Asia', 'QAT' = 'Asia', 'SAU' = 'Asia',
    'SGP' = 'Asia', 'KOR' = 'Asia', 'LKA' = 'Asia', 'SYR' = 'Asia', 'TJK' = 'Asia',
    'THA' = 'Asia', 'TLS' = 'Asia', 'TUR' = 'Asia', 'TKM' = 'Asia', 'ARE' = 'Asia',
    'UZB' = 'Asia', 'VNM' = 'Asia', 'YEM' = 'Asia',
    # Europe
    'ALB' = 'Europe', 'AUT' = 'Europe', 'BLR' = 'Europe', 'BEL' = 'Europe',
    'BIH' = 'Europe', 'BGR' = 'Europe', 'HRV' = 'Europe', 'CZE' = 'Europe', 'DNK' = 'Europe',
    'EST' = 'Europe', 'FIN' = 'Europe', 'FRA' = 'Europe', 'DEU' = 'Europe', 'GRC' = 'Europe',
    'HUN' = 'Europe', 'ISL' = 'Europe', 'IRL' = 'Europe', 'ITA' = 'Europe', 'LVA' = 'Europe',
    'LTU' = 'Europe', 'LUX' = 'Europe', 'MLT' = 'Europe', 'MDA' = 'Europe',
    'MNE' = 'Europe', 'NLD' = 'Europe', 'MKD' = 'Europe', 'NOR' = 'Europe',
    'POL' = 'Europe', 'PRT' = 'Europe', 'ROU' = 'Europe', 'RUS' = 'Europe',
    'SRB' = 'Europe', 'SVK' = 'Europe', 'SVN' = 'Europe', 'ESP' = 'Europe', 'SWE' = 'Europe',
    'CHE' = 'Europe', 'UKR' = 'Europe', 'GBR' = 'Europe',
    # North America
    'ATG' = 'North America', 'BHS' = 'North America', 'BRB' = 'North America', 'BLZ' = 'North America',
    'CAN' = 'North America', 'CRI' = 'North America', 'CUB' = 'North America', 'DMA' = 'North America',
    'DOM' = 'North America', 'SLV' = 'North America', 'GRD' = 'North America', 'GTM' = 'North America',
    'HTI' = 'North America', 'HND' = 'North America', 'JAM' = 'North America', 'MEX' = 'North America',
    'NIC' = 'North America', 'PAN' = 'North America', 'KNA' = 'North America', 'LCA' = 'North America',
    'VCT' = 'North America', 'TTO' = 'North America', 'USA' = 'North America',
    # South America
    'ARG' = 'South America', 'BOL' = 'South America', 'BRA' = 'South America', 'CHL' = 'South America',
    'COL' = 'South America', 'ECU' = 'South America', 'GUY' = 'South America', 'PRY' = 'South America',
    'PER' = 'South America', 'SUR' = 'South America', 'URY' = 'South America', 'VEN' = 'South America',
    # Oceania
    'AUS' = 'Oceania', 'FJI' = 'Oceania', 'KIR' = 'Oceania', 'MHL' = 'Oceania', 'FSM' = 'Oceania',
    'NRU' = 'Oceania', 'NZL' = 'Oceania', 'PLW' = 'Oceania', 'PNG' = 'Oceania', 'WSM' = 'Oceania',
    'SLB' = 'Oceania', 'TON' = 'Oceania', 'TUV' = 'Oceania', 'VUT' = 'Oceania'
  )
}


#' @title List available UNICEF dataflows
#' @description Alias for list_unicef_flows() with consistent naming.
#' @param max_retries Number of retry attempts (default: 3)
#' @return Tibble with columns: id, agency, version
#' @export
list_dataflows <- function(max_retries = 3) {
  list_unicef_flows(retry = max_retries)
}
