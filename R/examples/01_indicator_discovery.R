# 01_indicator_discovery.R - Discover Available Indicators
# ==========================================================
#
# Demonstrates how to search and discover UNICEF indicators.
# Matches: python/examples/01_indicator_discovery.py
#
# Examples:
#   1. List all categories
#   2. Search by keyword
#   3. Search within category
#   4. Get indicator info
#   5. List dataflows

source("../get_unicef.R")
source("../indicator_registry.R")

cat("======================================================================\n")
cat("01_indicator_discovery.R - Discover UNICEF Indicators\n")
cat("======================================================================\n")

# =============================================================================
# Example 1: List All Categories
# =============================================================================
cat("\n--- Example 1: List All Categories ---\n\n")

list_categories()

# =============================================================================
# Example 2: Search by Keyword
# =============================================================================
cat("\n--- Example 2: Search by Keyword ---\n")
cat("Searching for 'mortality'...\n\n")

search_indicators("mortality", limit = 5)

# =============================================================================
# Example 3: Search Within Category
# =============================================================================
cat("\n--- Example 3: Search Within Category ---\n")
cat("Searching in NUTRITION category...\n\n")

search_indicators(category = "NUTRITION", limit = 5)

# =============================================================================
# Example 4: Get Indicator Info
# =============================================================================
cat("\n--- Example 4: Get Indicator Info ---\n")
cat("Getting info for CME_MRY0T4...\n\n")

info <- get_indicator_info("CME_MRY0T4")
if (!is.null(info)) {
  cat(sprintf("Code: %s\n", info$code))
  cat(sprintf("Name: %s\n", info$name))
  cat(sprintf("Category: %s\n", info$category))
}

# =============================================================================
# Example 5: Auto-detect Dataflow
# =============================================================================
cat("\n--- Example 5: Auto-detect Dataflow ---\n")
cat("Detecting dataflows for various indicators...\n\n")

indicators <- c(
  "CME_MRY0T4",              # Child Mortality
  "NT_ANT_HAZ_NE2_MOD",      # Nutrition
  "ED_CR_L1_UIS_MOD",        # Education (needs override)
  "PT_F_20-24_MRD_U18_TND"   # Child Marriage (needs override)
)

for (ind in indicators) {
  df <- get_dataflow_for_indicator(ind)
  cat(sprintf("  %s -> %s\n", ind, df))
}

# =============================================================================
# Example 6: List Available Dataflows
# =============================================================================
cat("\n--- Example 6: List Available Dataflows ---\n\n")

flows <- list_dataflows()
cat(sprintf("Total dataflows: %d\n", nrow(flows)))
cat("\nKey dataflows:\n")
key_flows <- c("CME", "NUTRITION", "EDUCATION_UIS_SDG", "IMMUNISATION", "MNCH", "PT", "PT_CM", "PT_FGM")
print(flows[flows$id %in% key_flows, c("id", "agency")])

cat("\n======================================================================\n")
cat("Indicator Discovery Complete!\n")
cat("======================================================================\n")
