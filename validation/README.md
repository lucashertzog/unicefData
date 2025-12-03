# Validation

This folder contains scripts to validate that the R and Python packages produce identical outputs.

## Usage

```bash
cd validation
python validate_outputs.py
```

## What it does

1. Scans for matching CSV files in:
   - `python/test_output/*.csv` vs `R/test_output/*.csv`
   - `python/examples/*.csv` vs `R/examples/*.csv`
2. Skips metadata files (`test_dataflows.csv`, `test_indicators.csv`, `test_codelists.csv`)
3. Compares each matching pair for:
   - Row counts
   - Column names
   - Key column values (iso3, indicator, period) with numeric tolerance
   - Numeric values (with 0.001 tolerance)
4. Generates `validation_results.csv` with summary

## Output

```
validation/
├── validate_outputs.py      # Main validation script
├── validation_results.csv   # Results summary (generated)
└── README.md
```

## Workflow

1. Run tests in Python: `cd python/test_output && python run_tests.py`
2. Run tests in R: `cd R/test_output && Rscript run_tests.R`
3. Validate outputs match: `cd validation && python validate_outputs.py`
