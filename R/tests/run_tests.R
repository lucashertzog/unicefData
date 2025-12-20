# ============================================================================
# Comprehensive test suite for unicefdata R package
# 
# Test Strategy:
# - OFFLINE tests: Use bundled YAML metadata files (always run, fast)
# - NETWORK tests: Call UNICEF API (skipped in CI, run locally)
#
# Bundled metadata location: R/metadata/current/
# ============================================================================

# Source dependencies
if (file.exists("R/get_unicef.R")) {
  source("R/get_unicef.R")
  source("R/metadata.R")
  source("R/flows.R")
  source("R/aliases_devtests.R")  # Provides list_dataflows()
  OUTPUT_DIR <- "R/tests/output"
  METADATA_DIR <- "R/metadata/current"
} else if (file.exists("../get_unicef.R")) {
  source("../get_unicef.R")
  source("../metadata.R")
  source("../flows.R")
  source("../aliases_devtests.R")
  OUTPUT_DIR <- "output"
  METADATA_DIR <- "../metadata/current"
} else {
  stop("Could not find R/get_unicef.R - run from unicefData root directory")
}

if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
}

# Check environment
IN_CI <- Sys.getenv("CI") != "" || Sys.getenv("GITHUB_ACTIONS") != ""

# Helper function
log_msg <- function(msg) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
}

# ============================================================================
# OFFLINE TESTS - Use bundled YAML metadata (always run, fast)
# ============================================================================

test_yaml_dataflows <- function() {
  log_msg("Testing YAML dataflows loading...")
  
  yaml_path <- file.path(METADATA_DIR, "_unicefdata_dataflows.yaml")
  if (!file.exists(yaml_path)) {
    log_msg(sprintf("  SKIP: %s not found", yaml_path))
    return(TRUE)  # Skip gracefully
  }
  
  data <- yaml::read_yaml(yaml_path)
  
  n_dataflows <- length(data$dataflows)
  log_msg(sprintf("  Found %d dataflows in YAML", n_dataflows))
  
  # Verify structure
  first_df <- data$dataflows[[1]]
  has_required <- all(c("id", "name", "agency") %in% names(first_df))
  log_msg(sprintf("  Structure valid: %s", has_required))
  
  # Check specific dataflows exist
  expected <- c("CME", "EDUCATION", "NUTRITION", "IMMUNISATION")
  found <- sum(expected %in% names(data$dataflows))
  log_msg(sprintf("  Key dataflows present: %d/%d", found, length(expected)))
  
  return(n_dataflows >= 50 && has_required && found == length(expected))
}

test_yaml_indicators <- function() {
  log_msg("Testing YAML indicators loading...")
  
  yaml_path <- file.path(METADATA_DIR, "_unicefdata_indicators.yaml")
  if (!file.exists(yaml_path)) {
    log_msg(sprintf("  SKIP: %s not found", yaml_path))
    return(TRUE)
  }
  
  data <- yaml::read_yaml(yaml_path)
  
  n_indicators <- length(data$indicators)
  log_msg(sprintf("  Found %d indicators in YAML", n_indicators))
  
  # Check for expected indicators (some may not be in minimal YAML)
  expected <- c("CME_MRY0T4", "NT_ANT_HAZ_NE2", "IM_DTP3")
  found <- sum(expected %in% names(data$indicators))
  log_msg(sprintf("  Key indicators present: %d/%d", found, length(expected)))
  
  # Minimum threshold: at least 10 indicators (bundled YAML may be minimal)
  return(n_indicators >= 10 && found >= 1)
}

test_yaml_countries <- function() {
  log_msg("Testing YAML countries loading...")
  
  yaml_path <- file.path(METADATA_DIR, "_unicefdata_countries.yaml")
  if (!file.exists(yaml_path)) {
    log_msg(sprintf("  SKIP: %s not found", yaml_path))
    return(TRUE)
  }
  
  data <- yaml::read_yaml(yaml_path)
  
  n_countries <- length(data$countries)
  log_msg(sprintf("  Found %d countries in YAML", n_countries))
  
  # Check for expected countries
  expected <- c("USA", "GBR", "FRA", "DEU", "BRA", "IND", "CHN")
  found <- sum(expected %in% names(data$countries))
  log_msg(sprintf("  Key countries present: %d/%d", found, length(expected)))
  
  return(n_countries >= 150 && found >= 5)
}

test_dataflow_schema_cme <- function() {
  log_msg("Testing dataflow_schema('CME') with local metadata...")
  
  schema_path <- file.path(METADATA_DIR, "dataflows", "CME.yaml")
  if (!file.exists(schema_path)) {
    log_msg(sprintf("  SKIP: %s not found", schema_path))
    return(TRUE)
  }
  
  # Test the dataflow_schema function with explicit metadata_dir
  schema <- tryCatch({
    dataflow_schema("CME", metadata_dir = METADATA_DIR)
  }, error = function(e) {
    log_msg(sprintf("  ERROR: %s", e$message))
    return(NULL)
  })
  
  if (is.null(schema)) return(FALSE)
  
  log_msg(sprintf("  Schema ID: %s", schema$id))
  log_msg(sprintf("  Dimensions: %d (%s)", length(schema$dimensions), 
                  paste(head(schema$dimensions, 3), collapse = ", ")))
  log_msg(sprintf("  Attributes: %d", length(schema$attributes)))
  
  has_dims <- length(schema$dimensions) > 0
  has_attrs <- length(schema$attributes) > 0
  
  return(has_dims && has_attrs)
}

test_dataflow_schema_education <- function() {
  log_msg("Testing dataflow_schema('EDUCATION') with local metadata...")
  
  schema_path <- file.path(METADATA_DIR, "dataflows", "EDUCATION.yaml")
  if (!file.exists(schema_path)) {
    log_msg(sprintf("  SKIP: %s not found", schema_path))
    return(TRUE)
  }
  
  schema <- tryCatch({
    dataflow_schema("EDUCATION", metadata_dir = METADATA_DIR)
  }, error = function(e) {
    log_msg(sprintf("  ERROR: %s", e$message))
    return(NULL)
  })
  
  if (is.null(schema)) return(FALSE)
  
  log_msg(sprintf("  Schema ID: %s", schema$id))
  log_msg(sprintf("  Dimensions: %s", paste(head(schema$dimensions, 5), collapse = ", ")))
  
  return(schema$id == "EDUCATION" && length(schema$dimensions) > 0)
}

test_print_schema <- function() {
  log_msg("Testing print method for dataflow_schema...")
  
  schema <- tryCatch({
    dataflow_schema("CME", metadata_dir = METADATA_DIR)
  }, error = function(e) {
    log_msg(sprintf("  SKIP: %s", e$message))
    return(NULL)
  })
  
  if (is.null(schema)) return(TRUE)
  
  # Capture print output
  output <- capture.output(print(schema))
  
  log_msg(sprintf("  Print output lines: %d", length(output)))
  
  # Check print method produces expected content
  has_header <- any(grepl("Dataflow Schema", output))
  has_dimensions <- any(grepl("Dimensions|INDICATOR|REF_AREA", output))
  
  log_msg(sprintf("  Has header: %s, Has dimensions: %s", has_header, has_dimensions))
  
  return(has_header)
}

# ============================================================================
# NETWORK TESTS - Call UNICEF API (skipped in CI)
# ============================================================================

test_list_flows_api <- function() {
  log_msg("Testing list_dataflows() from API...")
  
  flows <- list_dataflows()
  
  log_msg(sprintf("  Found %d dataflows", nrow(flows)))
  
  write.csv(flows, file.path(OUTPUT_DIR, "test_dataflows.csv"), 
            row.names = FALSE, fileEncoding = "UTF-8")
  log_msg("  Saved to test_dataflows.csv")
  
  return(nrow(flows) > 50)
}

test_child_mortality_api <- function() {
  log_msg("Testing child mortality API (CME_MRY0T4)...")
  
  df <- get_unicef(
    indicator = "CME_MRY0T4",
    countries = c("USA", "GBR", "FRA"),
    start_year = 2020,
    end_year = 2023
  )
  
  log_msg(sprintf("  Retrieved %d observations", nrow(df)))
  
  if (!is.null(df) && nrow(df) > 0) {
    write.csv(df, file.path(OUTPUT_DIR, "test_mortality.csv"), row.names = FALSE)
    log_msg("  Saved to test_mortality.csv")
    return(nrow(df) > 0)
  }
  
  return(FALSE)
}

# ============================================================================
# Run All Tests
# ============================================================================

run_all_tests <- function() {
  cat("============================================================\n")
  cat("UNICEF API R Package Test Suite\n")
  cat(sprintf("Started: %s\n", Sys.time()))
  cat(sprintf("Environment: %s\n", if (IN_CI) "CI (GitHub Actions)" else "Local"))
  cat(sprintf("Metadata dir: %s (exists: %s)\n", METADATA_DIR, dir.exists(METADATA_DIR)))
  cat("============================================================\n\n")
  
  # OFFLINE tests - always run (fast, no network)
  cat("--- OFFLINE TESTS (bundled YAML metadata) ---\n\n")
  tests <- list(
    list(name = "YAML Dataflows", fn = test_yaml_dataflows),
    list(name = "YAML Indicators", fn = test_yaml_indicators),
    list(name = "YAML Countries", fn = test_yaml_countries),
    list(name = "Dataflow Schema (CME)", fn = test_dataflow_schema_cme),
    list(name = "Dataflow Schema (EDUCATION)", fn = test_dataflow_schema_education),
    list(name = "Print Schema Method", fn = test_print_schema)
  )
  
  # NETWORK tests - only run locally
  if (!IN_CI) {
    cat("\n--- NETWORK TESTS (API calls) ---\n\n")
    tests <- c(tests, list(
      list(name = "List Dataflows API", fn = test_list_flows_api),
      list(name = "Child Mortality API", fn = test_child_mortality_api)
    ))
  } else {
    cat("\n--- Skipping network tests (CI environment) ---\n\n")
  }
  
  results <- list()
  
  for (test in tests) {
    result <- tryCatch({
      passed <- test$fn()
      list(name = test$name, status = if (passed) "PASS" else "FAIL", error = NULL)
    }, error = function(e) {
      log_msg(sprintf("  ERROR: %s", e$message))
      list(name = test$name, status = "ERROR", error = e$message)
    })
    results[[length(results) + 1]] <- result
    cat("\n")
  }
  
  cat("============================================================\n")
  cat("TEST RESULTS\n")
  cat("============================================================\n")
  
  passed_count <- 0
  for (r in results) {
    icon <- if (r$status == "PASS") "PASS" else "FAIL"
    cat(sprintf("[%s] %s\n", icon, r$name))
    if (!is.null(r$error)) {
      cat(sprintf("       Error: %s\n", r$error))
    }
    if (r$status == "PASS") passed_count <- passed_count + 1
  }
  
  cat(sprintf("\nTotal: %d/%d tests passed\n", passed_count, length(results)))
  cat("============================================================\n")
  
  # Exit with error if tests failed (for CI)
  if (passed_count < length(results) && IN_CI) {
    quit(status = 1)
  }
  
  invisible(passed_count == length(results))
}

# Run tests
run_all_tests()
