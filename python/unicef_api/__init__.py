"""
unicef_api: Python library for downloading UNICEF indicators via SDMX API

This library provides a simplified interface for fetching child welfare and development 
indicators from UNICEF's SDMX data repository.

Main features:
- Download indicator data from UNICEF SDMX API
- Support for multiple dataflows (GLOBAL_DATAFLOW, CME, NUTRITION, EDUCATION, etc.)
- Automatic data cleaning and standardization
- Comprehensive error handling
- Caching support for offline work
- Country code validation

Basic usage:
    >>> from unicef_api import get_unicef
    >>> 
    >>> # Fetch under-5 mortality for specific countries
    >>> df = get_unicef(
    ...     indicator="CME_MRY0T4",
    ...     countries=["ALB", "USA", "BRA"],
    ...     start_year=2015,
    ...     end_year=2023
    ... )
    >>> 
    >>> # Fetch all countries, all years
    >>> df = get_unicef(indicator="NT_ANT_HAZ_NE2_MOD")

For more details, see: https://data.unicef.org/sdmx-api-documentation/
"""

__version__ = "0.3.0"
__author__ = "Joao Pedro Azevedo"
__email__ = "jazevedo@unicef.org"

from typing import List, Optional, Union
import pandas as pd

from unicef_api.sdmx_client import (
    UNICEFSDMXClient,
    SDMXError,
    SDMXBadRequestError,
    SDMXNotFoundError,
    SDMXServerError,
    SDMXAuthenticationError,
    SDMXForbiddenError,
    SDMXUnavailableError,
)

from unicef_api.config import (
    UNICEF_DATAFLOWS,
    COMMON_INDICATORS,
)

from unicef_api.indicator_registry import (
    get_dataflow_for_indicator,
    get_indicator_info,
    list_indicators,
    search_indicators,
    list_categories,
    refresh_indicator_cache,
    get_cache_info,
)

from unicef_api.utils import (
    validate_country_codes,
    validate_year_range,
    load_country_codes,
    clean_dataframe,
)

from unicef_api.metadata import (
    MetadataSync,
    DataflowMetadata,
    IndicatorMetadata,
    CodelistMetadata,
    sync_metadata,
    ensure_metadata,
    validate_indicator_data,
    list_vintages,
    compare_vintages,
)


# =============================================================================
# Unified get_unicef() function - Primary API
# =============================================================================

# Module-level client instance (lazy initialization)
_client: Optional[UNICEFSDMXClient] = None


def get_unicef(
    indicator: Union[str, List[str]],
    countries: Optional[List[str]] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    dataflow: Optional[str] = None,
    sex: str = "_T",
    tidy: bool = True,
    country_names: bool = True,
    max_retries: int = 3,
    # NEW: Post-production options
    format: str = "long",
    latest: bool = False,
    add_metadata: Optional[List[str]] = None,
    dropna: bool = False,
    simplify: bool = False,
    mrv: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch UNICEF indicator data from SDMX API.
    
    This is the primary function for downloading indicator data. It provides
    a simple, consistent interface matching the R package's get_unicef().
    
    Args:
        indicator: Indicator code(s). Single string or list of codes.
            Examples: "CME_MRY0T4" (under-5 mortality), "NT_ANT_HAZ_NE2_MOD" (stunting)
        countries: ISO 3166-1 alpha-3 country codes. If None, fetches all countries.
            Examples: ["ALB", "USA", "BRA"]
        start_year: First year of data (e.g., 2015). If None, fetches from earliest.
        end_year: Last year of data (e.g., 2023). If None, fetches to latest.
        dataflow: SDMX dataflow ID. If None, auto-detected from indicator.
            Examples: "CME", "NUTRITION", "EDUCATION_UIS_SDG"
        sex: Sex disaggregation filter.
            "_T" = Total (default), "F" = Female, "M" = Male, None = all
        tidy: If True, returns cleaned DataFrame with standardized columns.
            If False, returns raw API response.
        country_names: If True, adds country name column (requires tidy=True).
        max_retries: Number of retry attempts on network failure.
        
        # Post-production options:
        format: Output format. Options:
            - "long" (default): One row per observation
            - "wide": Countries as rows, years as columns (pivoted)
            - "wide_indicators": Years as rows, indicators as columns
        latest: If True, keep only the most recent non-missing value per country.
            The year may differ by country. Useful for cross-sectional analysis.
        add_metadata: List of metadata columns to add. Options:
            - "region": UNICEF/World Bank region
            - "income_group": World Bank income classification
            - "continent": Continent name
            - "indicator_name": Full indicator name
            - "indicator_category": Indicator category (CME, NUTRITION, etc.)
            Example: add_metadata=["region", "income_group"]
        dropna: If True, remove rows with missing values.
        simplify: If True, keep only essential columns (iso3, country, indicator, 
            period, value). Removes metadata columns.
        mrv: Most Recent Value(s). Keep only the N most recent years per country.
            Example: mrv=1 is equivalent to latest=True, mrv=3 keeps last 3 years.
    
    Returns:
        pandas.DataFrame with columns (varies by options):
            - iso3: ISO 3166-1 alpha-3 country code
            - country: Country name (if country_names=True)
            - indicator: Indicator code
            - period: Year
            - value: Observation value
            - region: Region (if add_metadata includes "region")
            - income_group: Income group (if add_metadata includes "income_group")
            - (additional columns depending on dataflow and options)
    
    Raises:
        SDMXNotFoundError: Indicator or country not found
        SDMXBadRequestError: Invalid parameters
        SDMXServerError: API server error
    
    Examples:
        >>> from unicef_api import get_unicef
        >>> 
        >>> # Basic usage - under-5 mortality for specific countries
        >>> df = get_unicef(
        ...     indicator="CME_MRY0T4",
        ...     countries=["ALB", "USA", "BRA"],
        ...     start_year=2015,
        ...     end_year=2023
        ... )
        >>> 
        >>> # Get latest value per country (cross-sectional)
        >>> df = get_unicef(
        ...     indicator="CME_MRY0T4",
        ...     latest=True
        ... )
        >>> 
        >>> # Wide format with region metadata
        >>> df = get_unicef(
        ...     indicator="CME_MRY0T4",
        ...     format="wide",
        ...     add_metadata=["region", "income_group"]
        ... )
        >>> 
        >>> # Multiple indicators merged automatically
        >>> df = get_unicef(
        ...     indicator=["CME_MRY0T4", "NT_ANT_HAZ_NE2_MOD"],
        ...     format="wide_indicators",
        ...     latest=True
        ... )
    
    See Also:
        - UNICEFSDMXClient: Advanced class-based interface
        - list_dataflows(): List available dataflows
        - search_indicators(): Find indicator codes
    """
    global _client
    
    # Lazy initialization of client
    if _client is None:
        _client = UNICEFSDMXClient()
    
    # Handle single indicator or list
    indicators = [indicator] if isinstance(indicator, str) else indicator
    
    # Auto-detect dataflow if not provided
    if dataflow is None:
        dataflow = get_dataflow_for_indicator(indicators[0])
    
    # Fetch each indicator
    dfs = []
    for ind in indicators:
        df = _client.fetch_indicator(
            indicator_code=ind,
            countries=countries,
            start_year=start_year,
            end_year=end_year,
            dataflow=dataflow,
            sex_disaggregation=sex,
            max_retries=max_retries,
            return_raw=not tidy,
        )
        if not df.empty:
            dfs.append(df)
    
    # Combine results
    if not dfs:
        return pd.DataFrame()
    
    result = pd.concat(dfs, ignore_index=True)
    
    # ==========================================================================
    # POST-PRODUCTION PROCESSING
    # ==========================================================================
    
    # Standardize column names for processing
    # Handle both raw SDMX names and cleaned names from SDMXClient
    col_mapping = {
        'REF_AREA': 'iso3',
        'country_code': 'iso3',
        'INDICATOR': 'indicator', 
        'indicator_code': 'indicator',
        'TIME_PERIOD': 'period',
        'year': 'period',
        'OBS_VALUE': 'value',
        'country_name': 'country',
    }
    for old, new in col_mapping.items():
        if old in result.columns and new not in result.columns:
            result = result.rename(columns={old: new})
    
    # Ensure period is numeric
    if 'period' in result.columns:
        result['period'] = pd.to_numeric(result['period'], errors='coerce')
    
    # 1. Add metadata columns
    if add_metadata and 'iso3' in result.columns:
        result = _add_country_metadata(result, add_metadata)
        result = _add_indicator_metadata(result, add_metadata)
    
    # 2. Drop NA values
    if dropna and 'value' in result.columns:
        result = result.dropna(subset=['value'])
    
    # 3. Most Recent Values (MRV)
    if mrv is not None and mrv > 0 and 'iso3' in result.columns and 'period' in result.columns:
        result = _apply_mrv(result, mrv)
    
    # 4. Latest value per country
    if latest and 'iso3' in result.columns and 'period' in result.columns:
        result = _apply_latest(result)
    
    # 5. Format transformation (long/wide)
    if format != "long" and 'iso3' in result.columns:
        result = _apply_format(result, format, indicators)
    
    # 6. Simplify columns
    if simplify:
        result = _simplify_columns(result, format)
    
    return result


def _add_country_metadata(df: pd.DataFrame, metadata_list: List[str]) -> pd.DataFrame:
    """Add country-level metadata columns."""
    try:
        import pycountry
    except ImportError:
        pycountry = None
    
    # Mapping of metadata names to data
    if 'region' in metadata_list:
        # UNICEF regions (simplified mapping)
        region_map = {
            'EAP': 'East Asia and Pacific',
            'ECA': 'Europe and Central Asia', 
            'LAC': 'Latin America and Caribbean',
            'MENA': 'Middle East and North Africa',
            'NA': 'North America',
            'SA': 'South Asia',
            'SSA': 'Sub-Saharan Africa',
            'WE': 'Western Europe',
        }
        # Basic ISO3 to region mapping (can be expanded)
        iso3_to_region = _get_country_regions()
        df['region'] = df['iso3'].map(iso3_to_region)
    
    if 'income_group' in metadata_list:
        income_map = _get_income_groups()
        df['income_group'] = df['iso3'].map(income_map)
    
    if 'continent' in metadata_list:
        continent_map = _get_continents()
        df['continent'] = df['iso3'].map(continent_map)
    
    return df


def _add_indicator_metadata(df: pd.DataFrame, metadata_list: List[str]) -> pd.DataFrame:
    """Add indicator-level metadata columns."""
    if 'indicator' not in df.columns:
        return df
    
    if 'indicator_name' in metadata_list or 'indicator_category' in metadata_list:
        # Get indicator info from registry
        unique_indicators = df['indicator'].unique()
        for ind in unique_indicators:
            info = get_indicator_info(ind)
            if info:
                if 'indicator_name' in metadata_list:
                    df.loc[df['indicator'] == ind, 'indicator_name'] = info.get('name', '')
                if 'indicator_category' in metadata_list:
                    df.loc[df['indicator'] == ind, 'indicator_category'] = info.get('category', '')
    
    return df


def _apply_mrv(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Keep only the N most recent values per country-indicator combination."""
    if 'indicator' in df.columns:
        # Group by country and indicator
        df = df.sort_values(['iso3', 'indicator', 'period'], ascending=[True, True, False])
        df = df.groupby(['iso3', 'indicator']).head(n).reset_index(drop=True)
    else:
        # Group by country only
        df = df.sort_values(['iso3', 'period'], ascending=[True, False])
        df = df.groupby('iso3').head(n).reset_index(drop=True)
    return df


def _apply_latest(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the latest non-missing value per country-indicator."""
    if 'value' in df.columns:
        df = df.dropna(subset=['value'])
    
    if 'indicator' in df.columns:
        # Get latest per country-indicator
        idx = df.groupby(['iso3', 'indicator'])['period'].idxmax()
    else:
        # Get latest per country
        idx = df.groupby('iso3')['period'].idxmax()
    
    return df.loc[idx].reset_index(drop=True)


def _apply_format(df: pd.DataFrame, format: str, indicators: List[str]) -> pd.DataFrame:
    """Transform between long and wide formats."""
    if format == "wide":
        # Countries as rows, years as columns
        # Only works well for single indicator
        if 'indicator' in df.columns and df['indicator'].nunique() > 1:
            print("Warning: 'wide' format with multiple indicators may produce complex output.")
            print("         Consider using 'wide_indicators' format instead.")
        
        # Identify columns to keep as index
        index_cols = ['iso3']
        if 'country' in df.columns:
            index_cols.append('country')
        for col in ['region', 'income_group', 'continent']:
            if col in df.columns:
                index_cols.append(col)
        if 'indicator' in df.columns and df['indicator'].nunique() > 1:
            index_cols.append('indicator')
        
        # Pivot
        df = df.pivot_table(
            index=index_cols,
            columns='period',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Flatten column names if multi-index
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [f"{a}_{b}" if b else a for a, b in df.columns]
    
    elif format == "wide_indicators":
        # Years as rows, indicators as columns
        # Useful when comparing multiple indicators
        if 'indicator' not in df.columns or df['indicator'].nunique() == 1:
            print("Warning: 'wide_indicators' format is designed for multiple indicators.")
            return df
        
        # Identify columns to keep as index
        index_cols = ['iso3', 'period']
        if 'country' in df.columns:
            index_cols.insert(1, 'country')
        for col in ['region', 'income_group', 'continent']:
            if col in df.columns:
                index_cols.append(col)
        
        # Pivot
        df = df.pivot_table(
            index=index_cols,
            columns='indicator',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Flatten column names
        df.columns.name = None
    
    return df


def _simplify_columns(df: pd.DataFrame, format: str) -> pd.DataFrame:
    """Keep only essential columns."""
    if format == "long":
        essential = ['iso3', 'country', 'indicator', 'period', 'value']
        available = [c for c in essential if c in df.columns]
        # Also keep metadata if added
        for col in ['region', 'income_group', 'continent', 'indicator_name']:
            if col in df.columns:
                available.append(col)
        return df[available]
    else:
        # For wide format, keep all pivoted columns
        return df


def _get_country_regions() -> dict:
    """Get ISO3 to UNICEF region mapping."""
    # Comprehensive mapping based on UNICEF regional classifications
    regions = {
        # East Asia and Pacific
        'AUS': 'East Asia and Pacific', 'BRN': 'East Asia and Pacific', 'KHM': 'East Asia and Pacific',
        'CHN': 'East Asia and Pacific', 'PRK': 'East Asia and Pacific', 'FJI': 'East Asia and Pacific',
        'IDN': 'East Asia and Pacific', 'JPN': 'East Asia and Pacific', 'KIR': 'East Asia and Pacific',
        'LAO': 'East Asia and Pacific', 'MYS': 'East Asia and Pacific', 'MHL': 'East Asia and Pacific',
        'FSM': 'East Asia and Pacific', 'MNG': 'East Asia and Pacific', 'MMR': 'East Asia and Pacific',
        'NRU': 'East Asia and Pacific', 'NZL': 'East Asia and Pacific', 'PLW': 'East Asia and Pacific',
        'PNG': 'East Asia and Pacific', 'PHL': 'East Asia and Pacific', 'WSM': 'East Asia and Pacific',
        'SGP': 'East Asia and Pacific', 'SLB': 'East Asia and Pacific', 'KOR': 'East Asia and Pacific',
        'THA': 'East Asia and Pacific', 'TLS': 'East Asia and Pacific', 'TON': 'East Asia and Pacific',
        'TUV': 'East Asia and Pacific', 'VUT': 'East Asia and Pacific', 'VNM': 'East Asia and Pacific',
        # Europe and Central Asia
        'ALB': 'Europe and Central Asia', 'ARM': 'Europe and Central Asia', 'AUT': 'Europe and Central Asia',
        'AZE': 'Europe and Central Asia', 'BLR': 'Europe and Central Asia', 'BEL': 'Europe and Central Asia',
        'BIH': 'Europe and Central Asia', 'BGR': 'Europe and Central Asia', 'HRV': 'Europe and Central Asia',
        'CYP': 'Europe and Central Asia', 'CZE': 'Europe and Central Asia', 'DNK': 'Europe and Central Asia',
        'EST': 'Europe and Central Asia', 'FIN': 'Europe and Central Asia', 'FRA': 'Europe and Central Asia',
        'GEO': 'Europe and Central Asia', 'DEU': 'Europe and Central Asia', 'GRC': 'Europe and Central Asia',
        'HUN': 'Europe and Central Asia', 'ISL': 'Europe and Central Asia', 'IRL': 'Europe and Central Asia',
        'ITA': 'Europe and Central Asia', 'KAZ': 'Europe and Central Asia', 'KGZ': 'Europe and Central Asia',
        'LVA': 'Europe and Central Asia', 'LTU': 'Europe and Central Asia', 'LUX': 'Europe and Central Asia',
        'MKD': 'Europe and Central Asia', 'MLT': 'Europe and Central Asia', 'MDA': 'Europe and Central Asia',
        'MNE': 'Europe and Central Asia', 'NLD': 'Europe and Central Asia', 'NOR': 'Europe and Central Asia',
        'POL': 'Europe and Central Asia', 'PRT': 'Europe and Central Asia', 'ROU': 'Europe and Central Asia',
        'RUS': 'Europe and Central Asia', 'SRB': 'Europe and Central Asia', 'SVK': 'Europe and Central Asia',
        'SVN': 'Europe and Central Asia', 'ESP': 'Europe and Central Asia', 'SWE': 'Europe and Central Asia',
        'CHE': 'Europe and Central Asia', 'TJK': 'Europe and Central Asia', 'TUR': 'Europe and Central Asia',
        'TKM': 'Europe and Central Asia', 'UKR': 'Europe and Central Asia', 'GBR': 'Europe and Central Asia',
        'UZB': 'Europe and Central Asia',
        # Latin America and Caribbean
        'ATG': 'Latin America and Caribbean', 'ARG': 'Latin America and Caribbean', 'BHS': 'Latin America and Caribbean',
        'BRB': 'Latin America and Caribbean', 'BLZ': 'Latin America and Caribbean', 'BOL': 'Latin America and Caribbean',
        'BRA': 'Latin America and Caribbean', 'CHL': 'Latin America and Caribbean', 'COL': 'Latin America and Caribbean',
        'CRI': 'Latin America and Caribbean', 'CUB': 'Latin America and Caribbean', 'DMA': 'Latin America and Caribbean',
        'DOM': 'Latin America and Caribbean', 'ECU': 'Latin America and Caribbean', 'SLV': 'Latin America and Caribbean',
        'GRD': 'Latin America and Caribbean', 'GTM': 'Latin America and Caribbean', 'GUY': 'Latin America and Caribbean',
        'HTI': 'Latin America and Caribbean', 'HND': 'Latin America and Caribbean', 'JAM': 'Latin America and Caribbean',
        'MEX': 'Latin America and Caribbean', 'NIC': 'Latin America and Caribbean', 'PAN': 'Latin America and Caribbean',
        'PRY': 'Latin America and Caribbean', 'PER': 'Latin America and Caribbean', 'KNA': 'Latin America and Caribbean',
        'LCA': 'Latin America and Caribbean', 'VCT': 'Latin America and Caribbean', 'SUR': 'Latin America and Caribbean',
        'TTO': 'Latin America and Caribbean', 'URY': 'Latin America and Caribbean', 'VEN': 'Latin America and Caribbean',
        # Middle East and North Africa
        'DZA': 'Middle East and North Africa', 'BHR': 'Middle East and North Africa', 'DJI': 'Middle East and North Africa',
        'EGY': 'Middle East and North Africa', 'IRN': 'Middle East and North Africa', 'IRQ': 'Middle East and North Africa',
        'ISR': 'Middle East and North Africa', 'JOR': 'Middle East and North Africa', 'KWT': 'Middle East and North Africa',
        'LBN': 'Middle East and North Africa', 'LBY': 'Middle East and North Africa', 'MAR': 'Middle East and North Africa',
        'OMN': 'Middle East and North Africa', 'QAT': 'Middle East and North Africa', 'SAU': 'Middle East and North Africa',
        'SDN': 'Middle East and North Africa', 'SYR': 'Middle East and North Africa', 'TUN': 'Middle East and North Africa',
        'ARE': 'Middle East and North Africa', 'YEM': 'Middle East and North Africa', 'PSE': 'Middle East and North Africa',
        # North America
        'CAN': 'North America', 'USA': 'North America',
        # South Asia
        'AFG': 'South Asia', 'BGD': 'South Asia', 'BTN': 'South Asia', 'IND': 'South Asia',
        'MDV': 'South Asia', 'NPL': 'South Asia', 'PAK': 'South Asia', 'LKA': 'South Asia',
        # Sub-Saharan Africa
        'AGO': 'Sub-Saharan Africa', 'BEN': 'Sub-Saharan Africa', 'BWA': 'Sub-Saharan Africa',
        'BFA': 'Sub-Saharan Africa', 'BDI': 'Sub-Saharan Africa', 'CPV': 'Sub-Saharan Africa',
        'CMR': 'Sub-Saharan Africa', 'CAF': 'Sub-Saharan Africa', 'TCD': 'Sub-Saharan Africa',
        'COM': 'Sub-Saharan Africa', 'COG': 'Sub-Saharan Africa', 'COD': 'Sub-Saharan Africa',
        'CIV': 'Sub-Saharan Africa', 'GNQ': 'Sub-Saharan Africa', 'ERI': 'Sub-Saharan Africa',
        'SWZ': 'Sub-Saharan Africa', 'ETH': 'Sub-Saharan Africa', 'GAB': 'Sub-Saharan Africa',
        'GMB': 'Sub-Saharan Africa', 'GHA': 'Sub-Saharan Africa', 'GIN': 'Sub-Saharan Africa',
        'GNB': 'Sub-Saharan Africa', 'KEN': 'Sub-Saharan Africa', 'LSO': 'Sub-Saharan Africa',
        'LBR': 'Sub-Saharan Africa', 'MDG': 'Sub-Saharan Africa', 'MWI': 'Sub-Saharan Africa',
        'MLI': 'Sub-Saharan Africa', 'MRT': 'Sub-Saharan Africa', 'MUS': 'Sub-Saharan Africa',
        'MOZ': 'Sub-Saharan Africa', 'NAM': 'Sub-Saharan Africa', 'NER': 'Sub-Saharan Africa',
        'NGA': 'Sub-Saharan Africa', 'RWA': 'Sub-Saharan Africa', 'STP': 'Sub-Saharan Africa',
        'SEN': 'Sub-Saharan Africa', 'SYC': 'Sub-Saharan Africa', 'SLE': 'Sub-Saharan Africa',
        'SOM': 'Sub-Saharan Africa', 'ZAF': 'Sub-Saharan Africa', 'SSD': 'Sub-Saharan Africa',
        'TZA': 'Sub-Saharan Africa', 'TGO': 'Sub-Saharan Africa', 'UGA': 'Sub-Saharan Africa',
        'ZMB': 'Sub-Saharan Africa', 'ZWE': 'Sub-Saharan Africa',
    }
    return regions


def _get_income_groups() -> dict:
    """Get ISO3 to World Bank income group mapping."""
    # World Bank income classifications (FY2024)
    income = {
        # High income
        'AUS': 'High income', 'AUT': 'High income', 'BEL': 'High income', 'CAN': 'High income',
        'CHE': 'High income', 'CHL': 'High income', 'CZE': 'High income', 'DEU': 'High income',
        'DNK': 'High income', 'ESP': 'High income', 'EST': 'High income', 'FIN': 'High income',
        'FRA': 'High income', 'GBR': 'High income', 'GRC': 'High income', 'HUN': 'High income',
        'IRL': 'High income', 'ISL': 'High income', 'ISR': 'High income', 'ITA': 'High income',
        'JPN': 'High income', 'KOR': 'High income', 'LTU': 'High income', 'LUX': 'High income',
        'LVA': 'High income', 'NLD': 'High income', 'NOR': 'High income', 'NZL': 'High income',
        'POL': 'High income', 'PRT': 'High income', 'SAU': 'High income', 'SGP': 'High income',
        'SVK': 'High income', 'SVN': 'High income', 'SWE': 'High income', 'USA': 'High income',
        'URY': 'High income', 'ARE': 'High income', 'BHR': 'High income', 'KWT': 'High income',
        'OMN': 'High income', 'QAT': 'High income', 'HRV': 'High income', 'CYP': 'High income',
        'MLT': 'High income', 'BRN': 'High income', 'TWN': 'High income', 'HKG': 'High income',
        'MAC': 'High income', 'PAN': 'High income', 'TTO': 'High income', 'BHS': 'High income',
        'BRB': 'High income', 'ATG': 'High income', 'KNA': 'High income', 'SYC': 'High income',
        'PLW': 'High income', 'NRU': 'High income', 'GUM': 'High income', 'PRI': 'High income',
        # Upper middle income
        'ARG': 'Upper middle income', 'BGR': 'Upper middle income', 'BRA': 'Upper middle income',
        'CHN': 'Upper middle income', 'COL': 'Upper middle income', 'CRI': 'Upper middle income',
        'DOM': 'Upper middle income', 'ECU': 'Upper middle income', 'GAB': 'Upper middle income',
        'GNQ': 'Upper middle income', 'GTM': 'Upper middle income', 'IRN': 'Upper middle income',
        'IRQ': 'Upper middle income', 'JAM': 'Upper middle income', 'JOR': 'Upper middle income',
        'KAZ': 'Upper middle income', 'LBN': 'Upper middle income', 'LBY': 'Upper middle income',
        'MEX': 'Upper middle income', 'MKD': 'Upper middle income', 'MNE': 'Upper middle income',
        'MUS': 'Upper middle income', 'MYS': 'Upper middle income', 'NAM': 'Upper middle income',
        'PER': 'Upper middle income', 'ROU': 'Upper middle income', 'RUS': 'Upper middle income',
        'SRB': 'Upper middle income', 'THA': 'Upper middle income', 'TUR': 'Upper middle income',
        'TKM': 'Upper middle income', 'VEN': 'Upper middle income', 'ZAF': 'Upper middle income',
        'ALB': 'Upper middle income', 'ARM': 'Upper middle income', 'AZE': 'Upper middle income',
        'BIH': 'Upper middle income', 'BWA': 'Upper middle income', 'CUB': 'Upper middle income',
        'DMA': 'Upper middle income', 'FJI': 'Upper middle income', 'GEO': 'Upper middle income',
        'GRD': 'Upper middle income', 'GUY': 'Upper middle income', 'LCA': 'Upper middle income',
        'MDV': 'Upper middle income', 'MHL': 'Upper middle income', 'PRY': 'Upper middle income',
        'SUR': 'Upper middle income', 'TON': 'Upper middle income', 'TUV': 'Upper middle income',
        'VCT': 'Upper middle income', 'XKX': 'Upper middle income',
        # Lower middle income
        'AGO': 'Lower middle income', 'BEN': 'Lower middle income', 'BGD': 'Lower middle income',
        'BLZ': 'Lower middle income', 'BOL': 'Lower middle income', 'BTN': 'Lower middle income',
        'CIV': 'Lower middle income', 'CMR': 'Lower middle income', 'COG': 'Lower middle income',
        'COM': 'Lower middle income', 'CPV': 'Lower middle income', 'DJI': 'Lower middle income',
        'DZA': 'Lower middle income', 'EGY': 'Lower middle income', 'GHA': 'Lower middle income',
        'HND': 'Lower middle income', 'HTI': 'Lower middle income', 'IDN': 'Lower middle income',
        'IND': 'Lower middle income', 'KEN': 'Lower middle income', 'KGZ': 'Lower middle income',
        'KHM': 'Lower middle income', 'KIR': 'Lower middle income', 'LAO': 'Lower middle income',
        'LKA': 'Lower middle income', 'LSO': 'Lower middle income', 'MAR': 'Lower middle income',
        'MDA': 'Lower middle income', 'MMR': 'Lower middle income', 'MNG': 'Lower middle income',
        'MRT': 'Lower middle income', 'NGA': 'Lower middle income', 'NIC': 'Lower middle income',
        'NPL': 'Lower middle income', 'PAK': 'Lower middle income', 'PHL': 'Lower middle income',
        'PNG': 'Lower middle income', 'PSE': 'Lower middle income', 'SEN': 'Lower middle income',
        'SLB': 'Lower middle income', 'SLV': 'Lower middle income', 'STP': 'Lower middle income',
        'SWZ': 'Lower middle income', 'TJK': 'Lower middle income', 'TLS': 'Lower middle income',
        'TUN': 'Lower middle income', 'TZA': 'Lower middle income', 'UKR': 'Lower middle income',
        'UZB': 'Lower middle income', 'VNM': 'Lower middle income', 'VUT': 'Lower middle income',
        'WSM': 'Lower middle income', 'ZMB': 'Lower middle income', 'ZWE': 'Lower middle income',
        # Low income
        'AFG': 'Low income', 'BDI': 'Low income', 'BFA': 'Low income', 'CAF': 'Low income',
        'COD': 'Low income', 'ERI': 'Low income', 'ETH': 'Low income', 'GMB': 'Low income',
        'GIN': 'Low income', 'GNB': 'Low income', 'LBR': 'Low income', 'MDG': 'Low income',
        'MLI': 'Low income', 'MOZ': 'Low income', 'MWI': 'Low income', 'NER': 'Low income',
        'PRK': 'Low income', 'RWA': 'Low income', 'SDN': 'Low income', 'SLE': 'Low income',
        'SOM': 'Low income', 'SSD': 'Low income', 'SYR': 'Low income', 'TCD': 'Low income',
        'TGO': 'Low income', 'UGA': 'Low income', 'YEM': 'Low income',
    }
    return income


def _get_continents() -> dict:
    """Get ISO3 to continent mapping."""
    continents = {
        # Africa
        'DZA': 'Africa', 'AGO': 'Africa', 'BEN': 'Africa', 'BWA': 'Africa', 'BFA': 'Africa',
        'BDI': 'Africa', 'CPV': 'Africa', 'CMR': 'Africa', 'CAF': 'Africa', 'TCD': 'Africa',
        'COM': 'Africa', 'COG': 'Africa', 'COD': 'Africa', 'CIV': 'Africa', 'DJI': 'Africa',
        'EGY': 'Africa', 'GNQ': 'Africa', 'ERI': 'Africa', 'SWZ': 'Africa', 'ETH': 'Africa',
        'GAB': 'Africa', 'GMB': 'Africa', 'GHA': 'Africa', 'GIN': 'Africa', 'GNB': 'Africa',
        'KEN': 'Africa', 'LSO': 'Africa', 'LBR': 'Africa', 'LBY': 'Africa', 'MDG': 'Africa',
        'MWI': 'Africa', 'MLI': 'Africa', 'MRT': 'Africa', 'MUS': 'Africa', 'MAR': 'Africa',
        'MOZ': 'Africa', 'NAM': 'Africa', 'NER': 'Africa', 'NGA': 'Africa', 'RWA': 'Africa',
        'STP': 'Africa', 'SEN': 'Africa', 'SYC': 'Africa', 'SLE': 'Africa', 'SOM': 'Africa',
        'ZAF': 'Africa', 'SSD': 'Africa', 'SDN': 'Africa', 'TZA': 'Africa', 'TGO': 'Africa',
        'TUN': 'Africa', 'UGA': 'Africa', 'ZMB': 'Africa', 'ZWE': 'Africa',
        # Asia
        'AFG': 'Asia', 'ARM': 'Asia', 'AZE': 'Asia', 'BHR': 'Asia', 'BGD': 'Asia',
        'BTN': 'Asia', 'BRN': 'Asia', 'KHM': 'Asia', 'CHN': 'Asia', 'CYP': 'Asia',
        'GEO': 'Asia', 'IND': 'Asia', 'IDN': 'Asia', 'IRN': 'Asia', 'IRQ': 'Asia',
        'ISR': 'Asia', 'JPN': 'Asia', 'JOR': 'Asia', 'KAZ': 'Asia', 'KWT': 'Asia',
        'KGZ': 'Asia', 'LAO': 'Asia', 'LBN': 'Asia', 'MYS': 'Asia', 'MDV': 'Asia',
        'MNG': 'Asia', 'MMR': 'Asia', 'NPL': 'Asia', 'PRK': 'Asia', 'OMN': 'Asia',
        'PAK': 'Asia', 'PSE': 'Asia', 'PHL': 'Asia', 'QAT': 'Asia', 'SAU': 'Asia',
        'SGP': 'Asia', 'KOR': 'Asia', 'LKA': 'Asia', 'SYR': 'Asia', 'TJK': 'Asia',
        'THA': 'Asia', 'TLS': 'Asia', 'TUR': 'Asia', 'TKM': 'Asia', 'ARE': 'Asia',
        'UZB': 'Asia', 'VNM': 'Asia', 'YEM': 'Asia',
        # Europe
        'ALB': 'Europe', 'AND': 'Europe', 'AUT': 'Europe', 'BLR': 'Europe', 'BEL': 'Europe',
        'BIH': 'Europe', 'BGR': 'Europe', 'HRV': 'Europe', 'CZE': 'Europe', 'DNK': 'Europe',
        'EST': 'Europe', 'FIN': 'Europe', 'FRA': 'Europe', 'DEU': 'Europe', 'GRC': 'Europe',
        'HUN': 'Europe', 'ISL': 'Europe', 'IRL': 'Europe', 'ITA': 'Europe', 'LVA': 'Europe',
        'LIE': 'Europe', 'LTU': 'Europe', 'LUX': 'Europe', 'MLT': 'Europe', 'MDA': 'Europe',
        'MCO': 'Europe', 'MNE': 'Europe', 'NLD': 'Europe', 'MKD': 'Europe', 'NOR': 'Europe',
        'POL': 'Europe', 'PRT': 'Europe', 'ROU': 'Europe', 'RUS': 'Europe', 'SMR': 'Europe',
        'SRB': 'Europe', 'SVK': 'Europe', 'SVN': 'Europe', 'ESP': 'Europe', 'SWE': 'Europe',
        'CHE': 'Europe', 'UKR': 'Europe', 'GBR': 'Europe', 'VAT': 'Europe',
        # North America
        'ATG': 'North America', 'BHS': 'North America', 'BRB': 'North America', 'BLZ': 'North America',
        'CAN': 'North America', 'CRI': 'North America', 'CUB': 'North America', 'DMA': 'North America',
        'DOM': 'North America', 'SLV': 'North America', 'GRD': 'North America', 'GTM': 'North America',
        'HTI': 'North America', 'HND': 'North America', 'JAM': 'North America', 'MEX': 'North America',
        'NIC': 'North America', 'PAN': 'North America', 'KNA': 'North America', 'LCA': 'North America',
        'VCT': 'North America', 'TTO': 'North America', 'USA': 'North America',
        # South America
        'ARG': 'South America', 'BOL': 'South America', 'BRA': 'South America', 'CHL': 'South America',
        'COL': 'South America', 'ECU': 'South America', 'GUY': 'South America', 'PRY': 'South America',
        'PER': 'South America', 'SUR': 'South America', 'URY': 'South America', 'VEN': 'South America',
        # Oceania
        'AUS': 'Oceania', 'FJI': 'Oceania', 'KIR': 'Oceania', 'MHL': 'Oceania', 'FSM': 'Oceania',
        'NRU': 'Oceania', 'NZL': 'Oceania', 'PLW': 'Oceania', 'PNG': 'Oceania', 'WSM': 'Oceania',
        'SLB': 'Oceania', 'TON': 'Oceania', 'TUV': 'Oceania', 'VUT': 'Oceania',
    }
    return continents


def list_dataflows(max_retries: int = 3) -> pd.DataFrame:
    """
    List all available UNICEF SDMX dataflows.
    
    Returns:
        DataFrame with columns: id, name, agency, version
    
    Example:
        >>> from unicef_api import list_dataflows
        >>> flows = list_dataflows()
        >>> print(flows.head())
    """
    import requests
    
    url = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/dataflow/UNICEF?references=none&detail=full"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            # Parse XML response
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            
            # Extract dataflows
            ns = {'s': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'}
            dataflows = []
            
            for df in root.findall('.//s:Dataflow', ns):
                name_elem = df.find('.//s:Name', ns)
                dataflows.append({
                    'id': df.get('id'),
                    'agency': df.get('agencyID'),
                    'version': df.get('version'),
                    'name': name_elem.text if name_elem is not None else ''
                })
            
            return pd.DataFrame(dataflows)
            
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            import time
            time.sleep(1)
    
    return pd.DataFrame()


__all__ = [
    # Primary API
    "get_unicef",
    "list_dataflows",
    # Client (advanced)
    "UNICEFSDMXClient",
    # Exceptions
    "SDMXError",
    "SDMXBadRequestError",
    "SDMXNotFoundError",
    "SDMXServerError",
    "SDMXAuthenticationError",
    "SDMXForbiddenError",
    "SDMXUnavailableError",
    # Config
    "UNICEF_DATAFLOWS",
    "COMMON_INDICATORS",
    "get_dataflow_for_indicator",
    # Utils
    "validate_country_codes",
    "validate_year_range",
    "load_country_codes",
    "clean_dataframe",
    # Metadata
    "MetadataSync",
    "DataflowMetadata",
    "IndicatorMetadata",
    "CodelistMetadata",
    "sync_metadata",
    "ensure_metadata",
    "validate_indicator_data",
    "list_vintages",
    "compare_vintages",
]
