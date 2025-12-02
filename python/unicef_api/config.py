"""
Configuration for UNICEF SDMX API
==================================

Dataflow definitions, indicator mappings, and API endpoint configurations.

This module provides both:
1. Hardcoded fallback indicator/dataflow definitions
2. Support for loading from shared config/indicators.yaml

The shared YAML config is the source of truth - hardcoded values are fallbacks.
"""

from typing import Dict, List, Optional
import os


# ============================================================================
# API Configuration
# ============================================================================

UNICEF_API_BASE_URL = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"
UNICEF_AGENCY = "UNICEF"
DEFAULT_VERSION = "1.0"


# ============================================================================
# Shared Config Loading
# ============================================================================

def _try_load_shared_config():
    """Try to load indicators from shared YAML config file.
    
    Returns:
        Tuple of (indicators_dict, dataflows_dict) or (None, None) if not found
    """
    try:
        from unicef_api.config_loader import load_indicators, load_dataflows
        return load_indicators(), load_dataflows()
    except (ImportError, FileNotFoundError):
        return None, None


# ============================================================================
# UNICEF Dataflows
# ============================================================================

UNICEF_DATAFLOWS = {
    # Global dataflow (contains most indicators)
    "GLOBAL_DATAFLOW": {
        "name": "GLOBAL_DATAFLOW",
        "version": "1.0",
        "description": "Global dataflow containing most UNICEF indicators",
    },
    
    # Specialized dataflows
    "CME": {
        "name": "CME",
        "version": "1.0",
        "description": "Child Mortality Estimates",
    },
    "NUTRITION": {
        "name": "NUTRITION",
        "version": "1.0",
        "description": "Nutrition indicators (stunting, wasting, overweight)",
    },
    "EDUCATION_UIS_SDG": {
        "name": "EDUCATION_UIS_SDG",
        "version": "1.0",
        "description": "Education indicators from UNESCO Institute for Statistics",
    },
    "IMMUNISATION": {
        "name": "IMMUNISATION",
        "version": "1.0",
        "description": "Immunization coverage indicators",
    },
    "HIV_AIDS": {
        "name": "HIV_AIDS",
        "version": "1.0",
        "description": "HIV/AIDS indicators",
    },
    "WASH_HOUSEHOLDS": {
        "name": "WASH_HOUSEHOLDS",
        "version": "1.0",
        "description": "Water, Sanitation, and Hygiene indicators",
    },
    "MNCH": {
        "name": "MNCH",
        "version": "1.0",
        "description": "Maternal, Newborn and Child Health indicators",
    },
    "PT": {
        "name": "PT",
        "version": "1.0",
        "description": "Child Protection indicators",
    },
    "PT_CM": {
        "name": "PT_CM",
        "version": "1.0",
        "description": "Child Marriage indicators",
    },
    "PT_FGM": {
        "name": "PT_FGM",
        "version": "1.0",
        "description": "Female Genital Mutilation indicators",
    },
    "ECD": {
        "name": "ECD",
        "version": "1.0",
        "description": "Early Childhood Development indicators",
    },
    "CHLD_PVTY": {
        "name": "CHLD_PVTY",
        "version": "1.0",
        "description": "Child Poverty indicators",
    },
}


# ============================================================================
# Common Indicators (SDG-related)
# ============================================================================

COMMON_INDICATORS = {
    # Mortality indicators (CME dataflow)
    "CME_MRM0": {
        "code": "CME_MRM0",
        "name": "Neonatal mortality rate",
        "dataflow": "CME",
        "sdg": "3.2.2",
        "unit": "Deaths per 1,000 live births",
    },
    "CME_MRY0T4": {
        "code": "CME_MRY0T4",
        "name": "Under-5 mortality rate",
        "dataflow": "CME",
        "sdg": "3.2.1",
        "unit": "Deaths per 1,000 live births",
    },
    
    # Nutrition indicators
    "NT_ANT_HAZ_NE2_MOD": {
        "code": "NT_ANT_HAZ_NE2_MOD",
        "name": "Stunting prevalence (moderate + severe)",
        "dataflow": "NUTRITION",
        "sdg": "2.2.1",
        "unit": "Percentage",
    },
    "NT_ANT_WHZ_NE2": {
        "code": "NT_ANT_WHZ_NE2",
        "name": "Wasting prevalence",
        "dataflow": "NUTRITION",
        "sdg": "2.2.2",
        "unit": "Percentage",
    },
    "NT_ANT_WHZ_PO2_MOD": {
        "code": "NT_ANT_WHZ_PO2_MOD",
        "name": "Overweight prevalence (moderate + severe)",
        "dataflow": "NUTRITION",
        "sdg": "2.2.2",
        "unit": "Percentage",
    },
    
    # Education indicators
    "ED_ANAR_L02": {
        "code": "ED_ANAR_L02",
        "name": "Adjusted net attendance rate, primary education",
        "dataflow": "EDUCATION_UIS_SDG",
        "sdg": "4.1.1",
        "unit": "Percentage",
    },
    "ED_CR_L1_UIS_MOD": {
        "code": "ED_CR_L1_UIS_MOD",
        "name": "Completion rate, primary education",
        "dataflow": "EDUCATION_UIS_SDG",
        "sdg": "4.1.1",
        "unit": "Percentage",
    },
    "ED_CR_L2_UIS_MOD": {
        "code": "ED_CR_L2_UIS_MOD",
        "name": "Completion rate, lower secondary education",
        "dataflow": "EDUCATION_UIS_SDG",
        "sdg": "4.1.1",
        "unit": "Percentage",
    },
    "ED_READ_L2": {
        "code": "ED_READ_L2",
        "name": "Reading proficiency, end of lower secondary",
        "dataflow": "EDUCATION_UIS_SDG",
        "sdg": "4.1.1",
        "unit": "Percentage",
    },
    "ED_MAT_L2": {
        "code": "ED_MAT_L2",
        "name": "Mathematics proficiency, end of lower secondary",
        "dataflow": "EDUCATION_UIS_SDG",
        "sdg": "4.1.1",
        "unit": "Percentage",
    },
    
    # Immunization
    "IM_DTP3": {
        "code": "IM_DTP3",
        "name": "DTP3 immunization coverage",
        "dataflow": "IMMUNISATION",
        "sdg": "3.b.1",
        "unit": "Percentage",
    },
    "IM_MCV1": {
        "code": "IM_MCV1",
        "name": "Measles immunization coverage (MCV1)",
        "dataflow": "IMMUNISATION",
        "sdg": "3.b.1",
        "unit": "Percentage",
    },
    
    # HIV/AIDS
    "HVA_EPI_INF_RT": {
        "code": "HVA_EPI_INF_RT",
        "name": "HIV incidence rate",
        "dataflow": "HIV_AIDS",
        "sdg": "3.3.1",
        "unit": "Per 1,000 uninfected population",
    },
    
    # WASH
    "WS_PPL_W-SM": {
        "code": "WS_PPL_W-SM",
        "name": "Population using safely managed drinking water services",
        "dataflow": "WASH_HOUSEHOLDS",
        "sdg": "6.1.1",
        "unit": "Percentage",
    },
    "WS_PPL_S-SM": {
        "code": "WS_PPL_S-SM",
        "name": "Population using safely managed sanitation services",
        "dataflow": "WASH_HOUSEHOLDS",
        "sdg": "6.2.1",
        "unit": "Percentage",
    },
    "WS_PPL_H-B": {
        "code": "WS_PPL_H-B",
        "name": "Population with basic handwashing facilities",
        "dataflow": "WASH_HOUSEHOLDS",
        "sdg": "6.2.1",
        "unit": "Percentage",
    },
    
    # Maternal and Child Health
    "MNCH_MMR": {
        "code": "MNCH_MMR",
        "name": "Maternal mortality ratio",
        "dataflow": "MNCH",
        "sdg": "3.1.1",
        "unit": "Deaths per 100,000 live births",
    },
    "MNCH_SAB": {
        "code": "MNCH_SAB",
        "name": "Skilled attendance at birth",
        "dataflow": "MNCH",
        "sdg": "3.1.2",
        "unit": "Percentage",
    },
    "MNCH_ABR": {
        "code": "MNCH_ABR",
        "name": "Adolescent birth rate",
        "dataflow": "MNCH",
        "sdg": "3.7.2",
        "unit": "Births per 1,000 women aged 15-19",
    },
    
    # Child Protection
    "PT_CHLD_Y0T4_REG": {
        "code": "PT_CHLD_Y0T4_REG",
        "name": "Birth registration (children under 5)",
        "dataflow": "PT",
        "sdg": "16.9.1",
        "unit": "Percentage",
    },
    "PT_CHLD_1-14_PS-PSY-V_CGVR": {
        "code": "PT_CHLD_1-14_PS-PSY-V_CGVR",
        "name": "Violent discipline (children 1-14)",
        "dataflow": "PT",
        "sdg": "16.2.1",
        "unit": "Percentage",
    },
    "PT_F_20-24_MRD_U18_TND": {
        "code": "PT_F_20-24_MRD_U18_TND",
        "name": "Child marriage before age 18 (women 20-24)",
        "dataflow": "PT_CM",
        "sdg": "5.3.1",
        "unit": "Percentage",
    },
    "PT_F_15-49_FGM": {
        "code": "PT_F_15-49_FGM",
        "name": "Female genital mutilation prevalence (women 15-49)",
        "dataflow": "PT_FGM",
        "sdg": "5.3.2",
        "unit": "Percentage",
    },
    
    # Early Childhood Development
    "ECD_CHLD_LMPSL": {
        "code": "ECD_CHLD_LMPSL",
        "name": "Children developmentally on track (literacy-numeracy, physical, social-emotional)",
        "dataflow": "ECD",
        "sdg": "4.2.1",
        "unit": "Percentage",
    },
    
    # Child Poverty
    "PV_CHLD_DPRV-S-L1-HS": {
        "code": "PV_CHLD_DPRV-S-L1-HS",
        "name": "Child multidimensional poverty (severe deprivation in at least 1 dimension)",
        "dataflow": "CHLD_PVTY",
        "sdg": "1.2.1",
        "unit": "Percentage",
    },
}


# ============================================================================
# Helper Functions
# ============================================================================

def get_dataflow_for_indicator(indicator_code: str) -> Optional[str]:
    """
    Get the appropriate dataflow for a given indicator code
    
    Args:
        indicator_code: UNICEF indicator code
    
    Returns:
        Dataflow name, or None if not found
        
    Example:
        >>> get_dataflow_for_indicator('CME_MRY0T4')
        'CME'
        >>> get_dataflow_for_indicator('NT_ANT_HAZ_NE2_MOD')
        'NUTRITION'
    """
    if indicator_code in COMMON_INDICATORS:
        return COMMON_INDICATORS[indicator_code].get("dataflow")
    
    # Try to infer from indicator code prefix
    if indicator_code.startswith("CME_"):
        return "CME"
    elif indicator_code.startswith("NT_"):
        return "NUTRITION"
    elif indicator_code.startswith("ED_"):
        return "EDUCATION_UIS_SDG"
    elif indicator_code.startswith("IM_"):
        return "IMMUNISATION"
    elif indicator_code.startswith("HVA_"):
        return "HIV_AIDS"
    elif indicator_code.startswith("WS_"):
        return "WASH_HOUSEHOLDS"
    elif indicator_code.startswith("MNCH_"):
        return "MNCH"
    elif indicator_code.startswith("PT_"):
        return "PT"
    elif indicator_code.startswith("ECD_"):
        return "ECD"
    elif indicator_code.startswith("PV_"):
        return "CHLD_PVTY"
    
    # Default to GLOBAL_DATAFLOW
    return "GLOBAL_DATAFLOW"


def get_indicator_metadata(indicator_code: str) -> Optional[Dict]:
    """
    Get metadata for a specific indicator
    
    Args:
        indicator_code: UNICEF indicator code
    
    Returns:
        Dictionary with indicator metadata, or None if not found
        
    Example:
        >>> meta = get_indicator_metadata('CME_MRY0T4')
        >>> print(meta['name'])
        'Under-5 mortality rate'
    """
    return COMMON_INDICATORS.get(indicator_code)


def list_indicators_by_sdg(sdg_target: str) -> List[str]:
    """
    List all indicators for a specific SDG target
    
    Args:
        sdg_target: SDG target number (e.g., '3.2.1')
    
    Returns:
        List of indicator codes
        
    Example:
        >>> indicators = list_indicators_by_sdg('3.2.1')
        >>> print(indicators)
        ['CME_MRY0T4']
    """
    return [
        code
        for code, meta in COMMON_INDICATORS.items()
        if meta.get("sdg") == sdg_target
    ]


def list_indicators_by_dataflow(dataflow: str) -> List[str]:
    """
    List all indicators for a specific dataflow
    
    Args:
        dataflow: Dataflow name (e.g., 'CME', 'NUTRITION')
    
    Returns:
        List of indicator codes
        
    Example:
        >>> indicators = list_indicators_by_dataflow('CME')
        >>> print(indicators)
        ['CME_MRM0', 'CME_MRY0T4']
    """
    return [
        code
        for code, meta in COMMON_INDICATORS.items()
        if meta.get("dataflow") == dataflow
    ]


def get_all_sdg_targets() -> List[str]:
    """
    Get list of all SDG targets covered by available indicators
    
    Returns:
        Sorted list of SDG targets
        
    Example:
        >>> targets = get_all_sdg_targets()
        >>> print(targets[:5])
        ['1.2.1', '2.2.1', '2.2.2', '3.1.1', '3.1.2']
    """
    targets = set()
    for meta in COMMON_INDICATORS.values():
        if "sdg" in meta:
            targets.add(meta["sdg"])
    return sorted(list(targets))


# ============================================================================
# Dynamic Config Loading (with fallback to hardcoded values)
# ============================================================================

def get_indicators(use_shared_config: bool = True) -> Dict[str, dict]:
    """Get indicator definitions, preferring shared config if available.
    
    Args:
        use_shared_config: If True, try to load from shared YAML config first
        
    Returns:
        Dictionary of indicator definitions
    """
    if use_shared_config:
        shared_indicators, _ = _try_load_shared_config()
        if shared_indicators:
            return shared_indicators
    return COMMON_INDICATORS


def get_dataflows(use_shared_config: bool = True) -> Dict[str, dict]:
    """Get dataflow definitions, preferring shared config if available.
    
    Args:
        use_shared_config: If True, try to load from shared YAML config first
        
    Returns:
        Dictionary of dataflow definitions
    """
    if use_shared_config:
        _, shared_dataflows = _try_load_shared_config()
        if shared_dataflows:
            return shared_dataflows
    return UNICEF_DATAFLOWS


def get_indicator_codes(
    category: Optional[str] = None,
    sdg_goal: Optional[str] = None,
    dataflow: Optional[str] = None,
    use_shared_config: bool = True
) -> List[str]:
    """Get list of indicator codes with optional filtering.
    
    Args:
        category: Filter by category (e.g., 'mortality', 'nutrition')
        sdg_goal: Filter by SDG goal (e.g., '3', '4')
        dataflow: Filter by dataflow (e.g., 'CME', 'NUTRITION')
        use_shared_config: If True, try to load from shared YAML config first
        
    Returns:
        List of indicator codes matching the filters
        
    Example:
        >>> codes = get_indicator_codes(category='mortality')
        ['CME_MRM0', 'CME_MRY0T4']
        
        >>> codes = get_indicator_codes(sdg_goal='3')
        ['CME_MRM0', 'CME_MRY0T4', 'IM_DTP3', 'IM_MCV1', ...]
    """
    indicators = get_indicators(use_shared_config)
    
    result = []
    for code, info in indicators.items():
        # Apply filters
        if category and info.get('category') != category:
            continue
        if sdg_goal and not info.get('sdg', '').startswith(f"{sdg_goal}."):
            continue
        if dataflow and info.get('dataflow') != dataflow:
            continue
        result.append(code)
    
    return sorted(result)
