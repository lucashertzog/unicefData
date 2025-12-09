# R Test Suite

This directory contains integration and functional tests for the R `unicefData` package. For unit tests (testthat), see `tests/testthat/`.

> **See also:** [validation/README.md](../../validation/README.md) for the complete testing infrastructure documentation.

## Quick Start

```powershell
# Run unit tests (testthat)
Rscript -e "setwd('C:/GitHub/others/unicefData'); devtools::test()"

# Run comprehensive integration tests
Rscript R/tests/run_tests.R
```

## Test Scripts

| Script | Description | Output |
|--------|-------------|--------|
| `run_tests.R` | Comprehensive test suite for all major functionality | `output/*.csv` |
| `test_prod_sdg_indicators.R` | SDG Report 2025 indicator validation | Console summary |
| `test_fallback.R` | Dataflow fallback logic tests | Console output |
| `test_git_unicef.R` | Raw API data fetch tests | `output/raw_api_data/*.csv` |

## `run_tests.R`

The main comprehensive test suite validates:
- Listing dataflows
- Fetching data for specific domains (Mortality, Stunting, Immunization)
- Handling multiple indicators
- Metadata synchronization

**Usage:**
```r
source("R/tests/run_tests.R")
run_all_tests()
```

**Output files:**
- `output/test_dataflows.csv`
- `output/test_mortality.csv`
- `output/test_stunting.csv`
- `output/test_immunization.csv`
- `output/test_multiple_indicators.csv`

## `test_prod_sdg_indicators.R`

Production-readiness test that downloads indicators for SDG Report 2025. Mirrors the Python test for cross-platform validation.

```r
source("R/tests/test_prod_sdg_indicators.R")
```

## `test_fallback.R`

Tests the dataflow resolution logic, verifying correct selection between specific dataflows (e.g., `CME`) and fallback to global dataflow.

```r
source("R/tests/test_fallback.R")
```

## `test_git_unicef.R`

Low-level API tests that fetch raw data for various flows (HIV, WASH, MNCH, Child Protection, etc.).

```r
source("R/tests/test_git_unicef.R")
```

**Output:** `output/raw_api_data/*.csv`

## Output Directory

All test artifacts are saved in `output/` (gitignored):
- `raw_api_data/` - Raw CSV downloads
- `metadata_sync_test/` - Metadata cache
- `*.csv` - Test output files

## Unit Tests (testthat)

Unit tests are in `tests/testthat/`:
- `test-unicefData.R` - Core function tests
- `test-available_indicators.R` - Indicator discovery
- `test-build_indicator_catalog.R` - Catalog building

Run with:
```powershell
Rscript -e "setwd('C:/GitHub/others/unicefData'); devtools::test()"
```
