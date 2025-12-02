# unicefdata# unicefdata



[![R-CMD-check](https://github.com/unicef-drp/unicefdata/actions/workflows/check.yaml/badge.svg)](https://github.com/unicef-drp/unicefdata/actions)[![R-CMD-check](https://github.com/your-org/unicefdata/actions/workflows/check.yaml/badge.svg)](https://github.com/your-org/unicefdata/actions)  

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/unicefdata)](https://cran.r-project.org/package=unicefdata)  

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)[![Codecov test coverage](https://codecov.io/gh/your-org/unicefdata/branch/main/graph/badge.svg)](https://codecov.io/gh/your-org/unicefdata)  



**Multi-language library for downloading UNICEF child welfare indicators via SDMX API**The **unicefdata** package provides a lightweight, consistent R interface to the UNICEF SDMX “Data Warehouse” API, inspired by `get_ilostat()` (ILO) and `wb_data()` (World Bank). You can fetch any indicator series simply by specifying its SDMX key, date range, and optional filters.



The **unicefdata** package provides lightweight, consistent interfaces to the [UNICEF SDMX Data Warehouse](https://sdmx.data.unicef.org/) in both **R** and **Python**. Inspired by `get_ilostat()` (ILO) and `wb_data()` (World Bank), you can fetch any indicator series simply by specifying its SDMX key, date range, and optional filters.---



---## ⚡️ Features



## 📂 Repository Structure- **`get_unicef()`** — download one or more SDMX series as a tidy `data.frame`  

- **`list_series()`** — browse available series codes and descriptions  

```- **Flexible parameters** for date range, geography, frequency, and output format  

unicefdata/- **Automatic caching** and retries  

├── R/                    # R package source code- **Built‐in error handling** for missing series, malformed URLs, empty results  

├── python/               # Python package source code- **`sanity_check()`** integration to track changes in raw CSVs  

│   ├── unicef_api/       # Python module

│   ├── examples/         # Usage examples---

│   └── tests/            # Unit tests

├── testthat/             # R unit tests## 🚀 Installation

├── DESCRIPTION           # R package metadata

└── README.md             # This fileFrom CRAN:

```

```r

---install.packages("unicefdata")



## ⚡ Features

# getUnicef

| Feature | R | Python |

|---------|---|--------|**Client for the UNICEF SDMX Data Warehouse**

| Download SDMX series as tidy data | ✅ | ✅ |

| Browse available series/dataflows | ✅ | ✅ |- **list_unicef_flows()**  

| Filter by country, year, sex | ✅ | ✅ |  Returns all available “flows” (tables) you can download.

| Automatic retries & error handling | ✅ | ✅ |

| 40+ pre-configured SDG indicators | ✅ | ✅ |- **list_unicef_codelist(flow, dimension)**  

| Batch download multiple indicators | ✅ | ✅ |  Returns the codelist (allowed codes + human‐readable descriptions) for a given flow + dimension.

| Data cleaning & transformation utilities | ✅ | ✅ |

- **get_unicef(flow, key = NULL, …)**  

---  Download one or more flows, with optional filters, automatic paging, retry, and tidy output.



## 🚀 Installation## Installation



### R Package```r

# From CRAN (once published)

```rinstall.packages("getUnicef")

# From GitHub:

# install.packages("devtools")# Or from GitHub:

devtools::install_github("unicef-drp/unicefdata")# install.packages("devtools")

```devtools::install_github("yourusername/getUnicef")


### Python Package

```bash
cd python
pip install -e .

# Or install dependencies directly:
pip install -r requirements.txt
```

---

## 🎯 Quick Start

### R

```r
library(unicefdata)

# List available dataflows
flows <- list_unicef_flows()

# Download under-5 mortality rate
df <- get_unicef(
  flow = "CME",
  key = "CME_MRY0T4",
  countries = c("ALB", "USA", "BRA"),
  start_year = 2015,
  end_year = 2023
)
```

### Python

```python
from unicef_api import UNICEFSDMXClient

# Initialize client
client = UNICEFSDMXClient()

# Download under-5 mortality rate
df = client.fetch_indicator(
    'CME_MRY0T4',
    countries=['ALB', 'USA', 'BRA'],
    start_year=2015,
    end_year=2023
)

print(df.head())
```

---

## 📊 Common Indicators

### Child Mortality (SDG 3.2)
| Indicator | Description |
|-----------|-------------|
| `CME_MRM0` | Neonatal mortality rate |
| `CME_MRY0T4` | Under-5 mortality rate |
| `CME_MRY0` | Infant mortality rate |

### Nutrition (SDG 2.2)
| Indicator | Description |
|-----------|-------------|
| `NT_ANT_HAZ_NE2_MOD` | Stunting prevalence (height-for-age) |
| `NT_ANT_WHZ_NE2` | Wasting prevalence (weight-for-height) |
| `NT_ANT_WHZ_PO2_MOD` | Overweight prevalence |
| `NT_BF_EXBF` | Exclusive breastfeeding rate |

### Education (SDG 4.1)
| Indicator | Description |
|-----------|-------------|
| `ED_CR_L1_UIS_MOD` | Primary completion rate |
| `ED_CR_L2_UIS_MOD` | Lower secondary completion rate |
| `ED_ANAR_L1` | Out-of-school rate (primary) |

### Immunization
| Indicator | Description |
|-----------|-------------|
| `IM_DTP3` | DTP3 immunization coverage |
| `IM_MCV1` | Measles immunization coverage |
| `IM_BCG` | BCG immunization coverage |

### Water & Sanitation (SDG 6)
| Indicator | Description |
|-----------|-------------|
| `WS_PPL_W-SM` | Population with safe drinking water |
| `WS_PPL_S-SM` | Population with basic sanitation |

---

## 📖 Documentation

- **Python**: See [`python/README.md`](python/README.md) for detailed Python documentation
- **Python Getting Started**: See [`python/GETTING_STARTED.md`](python/GETTING_STARTED.md)
- **R**: Function documentation via `?get_unicef` in R

---

## 🌐 UNICEF SDMX API

This package interfaces with UNICEF's SDMX Data Warehouse:
- **API Endpoint**: https://sdmx.data.unicef.org/ws/public/sdmxapi/rest
- **Query Builder**: https://sdmx.data.unicef.org/webservice/data.html
- **Data Explorer**: https://data.unicef.org/

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👥 Authors

- **Joao Pedro Azevedo** - *Lead Developer* - [azevedo.joaopedro@gmail.com](mailto:azevedo.joaopedro@gmail.com)
- **Garen Avanesian** - *Contributor*

---

## 🔗 Related Projects

- [wbdata](https://github.com/worldbank/wbdata) - World Bank Data API client
- [ilostat](https://github.com/ilostat/Rilostat) - ILO Statistics API client
- [rsdmx](https://github.com/opensdmx/rsdmx) - Generic SDMX client for R
- [pandaSDMX](https://pypi.org/project/pandaSDMX/) - Generic SDMX client for Python
