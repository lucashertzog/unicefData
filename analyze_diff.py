#!/usr/bin/env python
"""Analyze differences between Python and R outputs"""
import pandas as pd

print('=' * 80)
print('STUNTING - KEY DIFFERENCE ANALYSIS')
print('=' * 80)

py = pd.read_csv('python/test_output/test_stunting.csv')
r = pd.read_csv('R/test_output/test_stunting.csv')

print(f'Python: {len(py)} rows, R: {len(r)} rows')
print(f'Python data_source count: {py["data_source"].nunique()}')
print(f'R data_source count: {r["data_source"].nunique()}')
print()

# Count rows per country-period in each
print('--- Rows per country-period ---')
py_counts = py.groupby(['iso3', 'period']).size().reset_index(name='py_count')
r_counts = r.groupby(['iso3', 'period']).size().reset_index(name='r_count')
r_counts['period'] = r_counts['period'].astype(int)

merged = py_counts.merge(r_counts, on=['iso3', 'period'], how='outer').fillna(0)
merged['py_count'] = merged['py_count'].astype(int)
merged['r_count'] = merged['r_count'].astype(int)
merged['diff'] = merged['r_count'] - merged['py_count']
merged = merged.sort_values('diff', ascending=False)
print(merged[merged['diff'] != 0])

print()
print('=' * 80)
print('MULTIPLE INDICATORS - KEY DIFFERENCE ANALYSIS')
print('=' * 80)

py = pd.read_csv('python/test_output/test_multiple_indicators.csv')
r = pd.read_csv('R/test_output/test_multiple_indicators.csv')

print(f'Python: {len(py)} rows, R: {len(r)} rows')
print()

# Compare values for same keys
print('--- Value comparison for same indicator/country/period ---')
py_sorted = py.sort_values(['indicator', 'iso3', 'period']).reset_index(drop=True)
r_sorted = r.sort_values(['indicator', 'iso3', 'period']).reset_index(drop=True)

comparison = pd.DataFrame({
    'indicator': py_sorted['indicator'],
    'iso3': py_sorted['iso3'],
    'period': py_sorted['period'],
    'py_value': py_sorted['value'],
    'r_value': r_sorted['value'],
})
comparison['diff'] = comparison['py_value'] - comparison['r_value']
comparison['match'] = abs(comparison['diff']) < 0.001

print(f'Matching values: {comparison["match"].sum()} / {len(comparison)}')
print()
print('Rows with different values:')
print(comparison[~comparison['match']])
