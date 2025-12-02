# UNICEF Data Examples

This folder contains example scripts demonstrating how to use the `unicefdata` package.
Each example has an identical counterpart in `python/examples/`.

## Example Files

| File | Description |
|------|-------------|
| `00_quick_start.R` | Basic usage - fetching data for single/multiple indicators |
| `01_indicator_discovery.R` | Searching and exploring available indicators |
| `02_sdg_indicators.R` | SDG-related indicators across domains |
| `03_data_formats.R` | Output format options (long, wide, latest, MRV) |
| `04_metadata_options.R` | Adding metadata (region, income group, indicator name) |
| `05_advanced_features.R` | Disaggregation, time series, combining filters |
| `06_test_fallback.R` | Testing the dataflow fallback mechanism |

## Running Examples

```r
setwd("R/examples")
source("00_quick_start.R")
```

## Quick Reference

```r
source("../get_unicef.R")

# Basic fetch
df <- get_unicef("CME_MRY0T4", countries = c("ALB", "USA"))

# Multiple indicators
df <- get_unicef(c("CME_MRY0T4", "CME_MRM0"), countries = c("ALB"))

# Latest values only
df <- get_unicef("CME_MRY0T4", countries = c("ALB"), latest = TRUE)

# Wide format
df <- get_unicef("CME_MRY0T4", countries = c("ALB"), output_format = "wide")

# With metadata
df <- get_unicef("CME_MRY0T4", countries = c("ALB"), add_metadata = c("region", "income_group"))
```
