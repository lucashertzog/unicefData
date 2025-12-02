# test_api.R
# Quick test of UNICEF SDMX API functions

cat("========================================\n")
cat("Testing UNICEF SDMX R Functions\n")
cat("========================================\n\n")

# Set working directory
setwd("D:/jazevedo/GitHub/unicefData")

# Load required packages
cat("Loading packages...\n")
suppressPackageStartupMessages({
  library(httr)
  library(xml2)
  library(tibble)
  library(readr)
  library(dplyr)
  library(memoise)
  library(tools)
})

# Source the R functions
cat("Sourcing R functions...\n")
source("R/utils.R")
source("R/flows.R")
source("R/codelist.R")
source("R/get_sdmx.R")
source("R/data_utilities.R")

cat("\n--- Test 1: List UNICEF Dataflows ---\n")
tryCatch({
  flows <- list_sdmx_flows(agency = "UNICEF", retry = 3)
  cat(sprintf("✅ Success! Found %d dataflows\n", nrow(flows)))
  cat("\nFirst 10 dataflows:\n")
  print(head(flows, 10))
}, error = function(e) {
  cat(sprintf("❌ Error: %s\n", e$message))
})

cat("\n--- Test 2: Fetch Under-5 Mortality (CME_MRY0T4) ---\n")
tryCatch({
  mortality <- get_sdmx(
    agency       = "UNICEF",
    flow         = "CME",
    key          = "CME_MRY0T4",
    start_period = 2020,
    end_period   = 2023,
    format       = "csv",
    labels       = "both",
    tidy         = TRUE,
    country_names = TRUE,
    retry        = 3
  )
  cat(sprintf("✅ Success! Downloaded %d observations\n", nrow(mortality)))
  cat(sprintf("   Countries: %d\n", length(unique(mortality$iso3))))
  cat(sprintf("   Years: %s\n", paste(sort(unique(mortality$period)), collapse = ", ")))
  cat("\nSample data:\n")
  print(head(mortality[, c("iso3", "country", "indicator", "period", "value")], 5))
}, error = function(e) {
  cat(sprintf("❌ Error: %s\n", e$message))
})

cat("\n--- Test 3: Fetch Stunting Data (NUTRITION) ---\n")
tryCatch({
  stunting <- get_sdmx(
    agency       = "UNICEF",
    flow         = "NUTRITION",
    key          = "NT_ANT_HAZ_NE2_MOD",
    start_period = 2018,
    end_period   = 2023,
    format       = "csv",
    labels       = "both",
    tidy         = FALSE,  # Disable tidy
    retry        = 3
  )
  cat(sprintf("✅ Success! Downloaded %d observations\n", nrow(stunting)))
  cat(sprintf("   Countries: %d\n", length(unique(stunting$REF_AREA))))
  cat("\nSample data:\n")
  print(head(stunting[, c("REF_AREA", "INDICATOR", "TIME_PERIOD", "OBS_VALUE")], 5))
}, error = function(e) {
  cat(sprintf("❌ Error: %s\n", e$message))
})

cat("\n--- Test 4: Fetch Immunization Data (IM_DTP3) ---\n")
tryCatch({
  immun <- get_sdmx(
    agency       = "UNICEF",
    flow         = "IMMUNISATION",
    key          = "IM_DTP3",
    start_period = 2020,
    end_period   = 2023,
    format       = "csv",
    labels       = "both",
    tidy         = FALSE,  # Disable tidy
    retry        = 3
  )
  cat(sprintf("✅ Success! Downloaded %d observations\n", nrow(immun)))
  cat(sprintf("   Countries: %d\n", length(unique(immun$REF_AREA))))
  cat("\nSample data:\n")
  print(head(immun[, c("REF_AREA", "INDICATOR", "TIME_PERIOD", "OBS_VALUE")], 5))
}, error = function(e) {
  cat(sprintf("❌ Error: %s\n", e$message))
})

cat("\n========================================\n")
cat("Tests completed!\n")
cat("========================================\n")
