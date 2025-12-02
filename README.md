# unicefData

[![R-CMD-check](https://github.com/unicef-drp/unicefData/actions/workflows/check.yaml/badge.svg)](https://github.com/unicef-drp/unicefData/actions)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Bilingual R and Python library for downloading UNICEF child welfare indicators via SDMX API**

The **unicefData** package provides lightweight, consistent interfaces to the [UNICEF SDMX Data Warehouse](https://sdmx.data.unicef.org/) in both **R** and **Python**. Inspired by `get_ilostat()` (ILO) and `wb_data()` (World Bank), you can fetch any indicator series simply by specifying its SDMX key, date range, and optional filters.

---

## Quick Start

Both R and Python use the **same `get_unicef()` function** with identical parameter names.

### R

```r
library(unicefData)

# Fetch under-5 mortality for specific countries
df <- get_unicef(
  indicator = "CME_MRY0T4",
  dataflow = "CME",
  countries = c("ALB", "USA", "BRA"),
  start_year = 2015,
  end_year = 2023
)

print(head(df))
```

### Python

```python
from unicef_api import get_unicef

# Fetch under-5 mortality for specific countries
df = get_unicef(
    indicator="CME_MRY0T4",
    countries=["ALB", "USA", "BRA"],
    start_year=2015,
    end_year=2023
)

print(df.head())
```

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

## Unified API Reference

### get_unicef() Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `indicator` | string/vector | required | Indicator code(s), e.g., `CME_MRY0T4` |
| `dataflow` | string | auto-detect | SDMX dataflow ID, e.g., `CME`, `NUTRITION` |
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
| `GLOBAL_DATAFLOW` | All indicators | (default) |

Use `list_dataflows()` to see all 69+ available dataflows.

---

## Features

| Feature | R | Python |
|---------|---|--------|
| Unified `get_unicef()` API | Yes | Yes |
| Filter by country, year, sex | Yes | Yes |
| Automatic retries | Yes | Yes |
| Sync metadata to YAML | Yes | Yes |
| 25+ pre-configured SDG indicators | Yes | Yes |
| Country name lookup | Yes | Yes |
| Disk-based caching | Yes | No |

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
