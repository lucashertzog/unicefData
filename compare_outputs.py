#!/usr/bin/env python
"""Compare Python and R test outputs"""
import pandas as pd
import numpy as np

files = ['test_mortality.csv', 'test_immunization.csv', 'test_stunting.csv', 'test_multiple_indicators.csv']

print('=' * 90)
print('PYTHON vs R OUTPUT COMPARISON')
print('=' * 90)

for f in files:
    py = pd.read_csv(f'python/test_output/{f}')
    r = pd.read_csv(f'R/test_output/{f}')
    
    print(f'\n--- {f} ---')
    print(f'  Dimensions:    Python={py.shape[0]} rows x {py.shape[1]} cols | R={r.shape[0]} rows x {r.shape[1]} cols')
    print(f'  Indicators:    Python={py["indicator"].nunique()} | R={r["indicator"].nunique()}')
    print(f'  Countries:     Python={py["iso3"].nunique()} | R={r["iso3"].nunique()}')
    
    py_period_mean = py['period'].mean() if 'period' in py.columns else np.nan
    r_period_mean = r['period'].mean() if 'period' in r.columns else np.nan
    print(f'  Avg Period:    Python={py_period_mean:.2f} | R={r_period_mean:.2f}')
    
    py_value_mean = py['value'].mean() if 'value' in py.columns else np.nan
    r_value_mean = r['value'].mean() if 'value' in r.columns else np.nan
    print(f'  Avg Value:     Python={py_value_mean:.4f} | R={r_value_mean:.4f}')
    
    # Check match
    match_rows = py.shape[0] == r.shape[0]
    match_cols = py.shape[1] == r.shape[1]
    match_period = abs(py_period_mean - r_period_mean) < 0.01 if not (np.isnan(py_period_mean) or np.isnan(r_period_mean)) else False
    match_value = abs(py_value_mean - r_value_mean) < 0.0001 if not (np.isnan(py_value_mean) or np.isnan(r_value_mean)) else False
    
    status = 'MATCH' if (match_rows and match_cols and match_period and match_value) else 'DIFF'
    print(f'  Status:        {status}')

print('\n' + '=' * 90)
