# unicefData

[![R-CMD-check](https://github.com/unicef-drp/unicefData/actions/workflows/check.yaml/badge.svg)](https://github.com/unicef-drp/unicefData/actions)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Bilingual R and Python library for downloading UNICEF child welfare indicators via SDMX API**

The **unicefData** package provides lightweight, consistent interfaces to the [UNICEF SDMX Data Warehouse](https://sdmx.data.unicef.org/) in both **R** and **Python**. Inspired by `get_ilostat()` (ILO) and `wb_data()` (World Bank), you can fetch any indicator series simply by specifying its SDMX key, date range, and optional filters.

---

## Quick Start

Both R and Python use the **same `get_unicef()` function** with identical parameter names.

### Python

```python
from unicef_api import get_unicef

# Fetch under-5 mortality for specific countries
# Dataflow is auto-detected from the indicator code!
df = get_unicef(
    indicator="CME_MRY0T4",
    countries=["ALB", "USA", "BRA"],
    start_year=2015,
    end_year=2023
)

print(df.head())
```

### R

```r
library(unicefData)

# Fetch under-5 mortality for specific countries
# Dataflow is auto-detected from the indicator code!
df <- get_unicef(
  indicator = "CME_MRY0T4",
  countries = c("ALB", "USA", "BRA"),
  start_year = 2015,
  end_year = 2023
)

print(head(df))
```

> **Note:** You don't need to specify `dataflow`! The package automatically detects it from the indicator code on first use, fetching the complete indicator codelist from the UNICEF SDMX API.

---

## Installation

### R Package

```r
# Install from GitHub
devtools::install_github("unicef-drp/unicefData")
library(unicefData)
```

### Python Package

```bash
git clone https://github.com/unicef-drp/unicefData.git
cd unicefData/python
pip install -e .
```

---

## Automatic Dataflow Detection

The package automatically downloads the complete UNICEF indicator codelist (700+ indicators) on first use and caches it locally. This enables:

1. **No need to specify dataflow** - Just provide the indicator code
2. **Accurate mapping** - Each indicator maps to its correct dataflow
3. **Offline support** - Cache is saved to `config/unicef_indicators_metadata.yaml`
4. **Auto-refresh** - Cache is refreshed every 30 days

### How it works

```python
# Python
from unicef_api import get_dataflow_for_indicator

# Auto-detects dataflow from indicator code
get_dataflow_for_indicator("CME_MRY0T4")    # Returns: "CME"
get_dataflow_for_indicator("NT_ANT_HAZ_NE2_MOD")  # Returns: "NUTRITION"
get_dataflow_for_indicator("IM_DTP3")       # Returns: "IMMUNISATION"
```

```r
# R
source("R/indicator_registry.R")

# Auto-detects dataflow from indicator code
get_dataflow_for_indicator("CME_MRY0T4")    # Returns: "CME"
get_dataflow_for_indicator("NT_ANT_HAZ_NE2_MOD")  # Returns: "NUTRITION"
```

### Manual cache refresh

```python
# Python
from unicef_api import refresh_indicator_cache, get_cache_info

# Force refresh from API
n = refresh_indicator_cache()
print(f"Refreshed cache with {n} indicators")

# Check cache status
info = get_cache_info()
print(info)
```

```r
# R
source("R/indicator_registry.R")

# Force refresh from API
n <- refresh_indicator_cache()
message(sprintf("Refreshed cache with %d indicators", n))

# Check cache status
info <- get_cache_info()
print(info)
```

---

## Unified API Reference

### get_unicef() Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `indicator` | string/vector | required | Indicator code(s), e.g., `CME_MRY0T4` |
| `dataflow` | string | **auto-detect** | SDMX dataflow ID (optional - auto-detected from indicator) |
| `countries` | vector/list | NULL (all) | ISO3 country codes, e.g., `["ALB", "USA"]` |
| `start_year` | integer | NULL (all) | First year of data |
| `end_year` | integer | NULL (all) | Last year of data |
| `sex` | string | `_T` | Sex filter: `_T` (total), `F`, `M`, or NULL (all) |
| `tidy` | boolean | TRUE | Return cleaned data with standardized columns |
| `country_names` | boolean | TRUE | Add country name column |
| `max_retries` | integer | 3 | Number of retry attempts on failure |

---

## Available Dataflows

| Dataflow | Description | Example Indicators |
|----------|-------------|-------------------|
| `CME` | Child Mortality Estimates | `CME_MRY0T4`, `CME_MRM0` |
| `NUTRITION` | Nutrition indicators | `NT_ANT_HAZ_NE2_MOD` |
| `EDUCATION_UIS_SDG` | Education (UNESCO) | `ED_ANAR_L02` |
| `IMMUNISATION` | Immunization coverage | `IM_DTP3`, `IM_MCV1` |
| `MNCH` | Maternal and Child Health | `MNCH_MMR` |
| `WASH_HOUSEHOLDS` | Water and Sanitation | `WS_PPL_W-SM` |
| `PT` | Child Protection | `PT_CHLD_Y0T4_REG` |
| `GLOBAL_DATAFLOW` | All indicators | (fallback) |

Use `list_dataflows()` to see all 69+ available dataflows.

---

## Features

| Feature | R | Python |
|---------|---|--------|
| Unified `get_unicef()` API | ✅ | ✅ |
| **Auto dataflow detection** | ✅ | ✅ |
| Filter by country, year, sex | ✅ | ✅ |
| Automatic retries | ✅ | ✅ |
| Indicator metadata cache | ✅ | ✅ |
| 700+ indicators supported | ✅ | ✅ |
| Country name lookup | ✅ | ✅ |
| Disk-based caching | ✅ | No |

---

## Backward Compatibility (R)

Legacy parameter names still work:

| Legacy | New |
|--------|-----|
| `flow` | `dataflow` |
| `key` | `indicator` |
| `start_period` | `start_year` |
| `end_period` | `end_year` |
| `retry` | `max_retries` |

---

## Examples

See the examples directories:

- **R**: `R/examples/00_quick_start.R`
- **Python**: `python/examples/00_quick_start.py`

---

## Links

- UNICEF Data Portal: https://data.unicef.org/
- SDMX API Docs: https://data.unicef.org/sdmx-api-documentation/
- GitHub: https://github.com/unicef-drp/unicefData

---

## License

MIT License
