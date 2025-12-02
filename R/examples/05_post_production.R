# 05_post_production.R
# Post-Production Features Demo for unicefdata R Package
# 
# This script demonstrates all post-production features available
# in the get_unicef() function:
#
# - latest: Keep only latest non-missing value per country
# - mrv: Keep N most recent values per country
# - format: Transform between long/wide formats
# - add_metadata: Add region, income_group, indicator_name
# - simplify: Keep only essential columns
# - dropna: Remove missing values
#
# Author: Joao Pedro Azevedo
# Date: December 2024

# Setup
setwd("D:/jazevedo/GitHub/unicefData")
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
})
source("R/indicator_registry.R")
source("R/get_unicef.R")

cat("======================================================================\n")
cat("unicefdata Post-Production Features Demo\n")
cat("======================================================================\n\n")

# =============================================================================
# 1. LATEST - Get most recent value per country
# =============================================================================
cat("\n======================================================================\n")
cat("1. LATEST - Most Recent Value Per Country\n")
cat("======================================================================\n")
cat("Use case: Cross-sectional analysis where you need one value per country\n\n")

df <- get_unicef(
  indicator = "CME_MRY0T4",
  countries = c("ALB", "USA", "BRA", "IND", "NGA"),
  start_year = 2015,
  latest = TRUE
)

cat(sprintf("Shape: %d x %d\n", nrow(df), ncol(df)))
cat("\nResult (one row per country, year may differ):\n")
print(df[, c("iso3", "country", "period", "value")])

# =============================================================================
# 2. MRV - Most Recent N Values per country
# =============================================================================
cat("\n======================================================================\n")
cat("2. MRV - Most Recent N Values Per Country\n")
cat("======================================================================\n")
cat("Use case: Time series analysis with N most recent data points\n\n")

df <- get_unicef(
  indicator = "CME_MRY0T4",
  countries = c("ALB", "USA"),
  start_year = 2010,
  mrv = 3  # Keep 3 most recent years
)

cat(sprintf("Shape: %d x %d (expect 6 rows: 3 years x 2 countries)\n", nrow(df), ncol(df)))
cat("\nResult:\n")
print(df[, c("iso3", "period", "value")])

# =============================================================================
# 3. FORMAT - Wide Format (Years as Columns)
# =============================================================================
cat("\n======================================================================\n")
cat("3. FORMAT='wide' - Years as Columns\n")
cat("======================================================================\n")
cat("Use case: Panel data analysis, Excel-like format\n\n")

df <- get_unicef(
  indicator = "CME_MRY0T4",
  countries = c("ALB", "USA", "BRA", "IND", "NGA"),
  start_year = 2020,
  format = "wide"
)

cat(sprintf("Shape: %d x %d\n", nrow(df), ncol(df)))
cat("\nResult (countries as rows, years as columns):\n")
print(df)

# =============================================================================
# 4. FORMAT - Wide Indicators (Indicators as Columns)
# =============================================================================
cat("\n======================================================================\n")
cat("4. FORMAT='wide_indicators' - Indicators as Columns\n")
cat("======================================================================\n")
cat("Use case: Compare multiple indicators side-by-side\n\n")

df <- get_unicef(
  indicator = c("CME_MRY0T4", "CME_MRM0"),  # Under-5 and Neonatal mortality
  countries = c("ALB", "USA", "BRA"),
  start_year = 2020,
  format = "wide_indicators"
)

cat(sprintf("Shape: %d x %d\n", nrow(df), ncol(df)))
cat("\nResult (indicators as columns):\n")
print(df)

# =============================================================================
# 5. ADD_METADATA - Enrich with Country Metadata
# =============================================================================
cat("\n======================================================================\n")
cat("5. ADD_METADATA - Country and Indicator Metadata\n")
cat("======================================================================\n")
cat("Use case: Regional analysis, grouping by income level\n\n")

df <- get_unicef(
  indicator = "CME_MRY0T4",
  countries = c("ALB", "USA", "BRA", "IND", "NGA", "JPN", "ZAF"),
  start_year = 2023,
  latest = TRUE,
  add_metadata = c("region", "income_group")
)

cat(sprintf("Shape: %d x %d\n", nrow(df), ncol(df)))
cat("\nColumns:", paste(names(df), collapse = ", "), "\n")
cat("\nResult with metadata:\n")
print(df[, c("iso3", "country", "value", "region", "income_group")])

# Example: Group by region
cat("\n--- Average by Region ---\n")
by_region <- df %>%
  group_by(region) %>%
  summarize(mean_value = mean(value, na.rm = TRUE)) %>%
  arrange(mean_value)
print(by_region)

# =============================================================================
# 6. SIMPLIFY - Keep Only Essential Columns
# =============================================================================
cat("\n======================================================================\n")
cat("6. SIMPLIFY - Minimal Output\n")
cat("======================================================================\n")
cat("Use case: Clean output for reporting or export\n\n")

df <- get_unicef(
  indicator = "CME_MRY0T4",
  countries = c("ALB", "USA", "BRA"),
  start_year = 2022,
  simplify = TRUE
)

cat(sprintf("Shape: %d x %d\n", nrow(df), ncol(df)))
cat("Columns:", paste(names(df), collapse = ", "), "\n")
cat("\nSimplified result:\n")
print(df)

# =============================================================================
# 7. COMBINED - Multiple Features Together
# =============================================================================
cat("\n======================================================================\n")
cat("7. COMBINED - Multiple Features Together\n")
cat("======================================================================\n")
cat("Use case: Comprehensive cross-sectional analysis\n\n")

df <- get_unicef(
  indicator = c("CME_MRY0T4", "CME_MRM0"),  # Mortality indicators
  countries = c("ALB", "USA", "BRA", "IND", "NGA", "JPN", "ZAF"),
  start_year = 2015,
  latest = TRUE,
  format = "wide_indicators",
  add_metadata = c("region", "income_group"),
  dropna = TRUE
)

cat(sprintf("Shape: %d x %d\n", nrow(df), ncol(df)))
cat("Columns:", paste(names(df), collapse = ", "), "\n")
cat("\nResult:\n")
print(df)

cat("\n======================================================================\n")
cat("Demo Complete!\n")
cat("======================================================================\n")
