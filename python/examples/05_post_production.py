"""
Post-Production Features Demo for unicef_api Python Package
=============================================================

This script demonstrates all post-production features available
in the get_unicef() function:

- latest: Keep only latest non-missing value per country
- mrv: Keep N most recent values per country
- format: Transform between long/wide formats
- add_metadata: Add region, income_group, indicator_name
- simplify: Keep only essential columns
- dropna: Remove missing values

Author: Joao Pedro Azevedo
Date: December 2024
"""

from unicef_api import get_unicef, search_indicators, list_categories
import pandas as pd

def main():
    print("=" * 70)
    print("unicef_api Post-Production Features Demo")
    print("=" * 70)
    print()

    # =========================================================================
    # 1. LATEST - Get most recent value per country
    # =========================================================================
    print("\n" + "=" * 70)
    print("1. LATEST - Most Recent Value Per Country")
    print("=" * 70)
    print("Use case: Cross-sectional analysis where you need one value per country")
    print()

    df = get_unicef(
        indicator="CME_MRY0T4",
        countries=["ALB", "USA", "BRA", "IND", "NGA"],
        start_year=2015,
        latest=True
    )

    print(f"Shape: {df.shape}")
    print("\nResult (one row per country, year may differ):")
    print(df[["iso3", "country", "period", "value"]].to_string())

    # =========================================================================
    # 2. MRV - Most Recent N Values per country
    # =========================================================================
    print("\n" + "=" * 70)
    print("2. MRV - Most Recent N Values Per Country")
    print("=" * 70)
    print("Use case: Time series analysis with N most recent data points")
    print()

    df = get_unicef(
        indicator="CME_MRY0T4",
        countries=["ALB", "USA"],
        start_year=2010,
        mrv=3  # Keep 3 most recent years
    )

    print(f"Shape: {df.shape} (expect 6 rows: 3 years x 2 countries)")
    print("\nResult:")
    print(df[["iso3", "period", "value"]].to_string())

    # =========================================================================
    # 3. FORMAT - Wide Format (Years as Columns)
    # =========================================================================
    print("\n" + "=" * 70)
    print("3. FORMAT='wide' - Years as Columns")
    print("=" * 70)
    print("Use case: Panel data analysis, Excel-like format")
    print()

    df = get_unicef(
        indicator="CME_MRY0T4",
        countries=["ALB", "USA", "BRA", "IND", "NGA"],
        start_year=2020,
        format="wide"
    )

    print(f"Shape: {df.shape}")
    print("\nResult (countries as rows, years as columns):")
    print(df.to_string())

    # =========================================================================
    # 4. FORMAT - Wide Indicators (Indicators as Columns)
    # =========================================================================
    print("\n" + "=" * 70)
    print("4. FORMAT='wide_indicators' - Indicators as Columns")
    print("=" * 70)
    print("Use case: Compare multiple indicators side-by-side")
    print()

    df = get_unicef(
        indicator=["CME_MRY0T4", "CME_MRM0"],  # Under-5 and Neonatal mortality
        countries=["ALB", "USA", "BRA"],
        start_year=2020,
        format="wide_indicators"
    )

    print(f"Shape: {df.shape}")
    print("\nResult (indicators as columns):")
    print(df.to_string())

    # =========================================================================
    # 5. ADD_METADATA - Enrich with Country Metadata
    # =========================================================================
    print("\n" + "=" * 70)
    print("5. ADD_METADATA - Country and Indicator Metadata")
    print("=" * 70)
    print("Use case: Regional analysis, grouping by income level")
    print()

    df = get_unicef(
        indicator="CME_MRY0T4",
        countries=["ALB", "USA", "BRA", "IND", "NGA", "JPN", "ZAF"],
        start_year=2023,
        latest=True,
        add_metadata=["region", "income_group", "indicator_name"]
    )

    print(f"Shape: {df.shape}")
    print("\nColumns:", df.columns.tolist())
    print("\nResult with metadata:")
    cols = ["iso3", "country", "value", "region", "income_group"]
    print(df[cols].to_string())

    # Example: Group by region
    print("\n--- Average by Region ---")
    by_region = df.groupby("region")["value"].mean().sort_values()
    print(by_region.to_string())

    # =========================================================================
    # 6. SIMPLIFY - Keep Only Essential Columns
    # =========================================================================
    print("\n" + "=" * 70)
    print("6. SIMPLIFY - Minimal Output")
    print("=" * 70)
    print("Use case: Clean output for reporting or export")
    print()

    df = get_unicef(
        indicator="CME_MRY0T4",
        countries=["ALB", "USA", "BRA"],
        start_year=2022,
        simplify=True
    )

    print(f"Shape: {df.shape}")
    print("Columns:", df.columns.tolist())
    print("\nSimplified result:")
    print(df.to_string())

    # =========================================================================
    # 7. COMBINED - Multiple Features Together
    # =========================================================================
    print("\n" + "=" * 70)
    print("7. COMBINED - Multiple Features Together")
    print("=" * 70)
    print("Use case: Comprehensive cross-sectional analysis")
    print()

    df = get_unicef(
        indicator=["CME_MRY0T4", "NT_ANT_HAZ_NE2_MOD"],  # Mortality + Stunting
        start_year=2015,
        latest=True,
        format="wide_indicators",
        add_metadata=["region", "income_group"],
        dropna=True
    )

    print(f"Shape: {df.shape}")
    print("Columns:", df.columns.tolist())
    print("\nSample (first 10 rows):")
    cols = ["iso3", "country", "region", "income_group", "CME_MRY0T4", "NT_ANT_HAZ_NE2_MOD"]
    available_cols = [c for c in cols if c in df.columns]
    print(df[available_cols].head(10).to_string())

    print()
    print("=" * 70)
    print("Demo Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
