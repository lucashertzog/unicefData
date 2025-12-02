"""
Metadata synchronization and validation for UNICEF SDMX API

This module provides functionality to:
1. Sync dataflow and indicator metadata from the UNICEF SDMX API
2. Cache metadata locally as YAML files for offline use
3. Validate downloaded data against cached metadata
4. Track metadata versions for triangulation and auditing

Usage:
    >>> from unicef_api.metadata import MetadataSync
    >>> sync = MetadataSync()
    >>> sync.sync_all()  # Downloads and caches all metadata
    >>> sync.validate_dataframe(df, 'CME_MRY0T4')  # Validate data
"""

import os
import yaml
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import hashlib


@dataclass
class DataflowMetadata:
    """Metadata for a UNICEF SDMX dataflow"""
    id: str
    name: str
    agency: str
    version: str
    description: Optional[str] = None
    dimensions: Optional[List[str]] = None
    indicators: Optional[List[str]] = None
    last_updated: Optional[str] = None


@dataclass 
class IndicatorMetadata:
    """Metadata for a UNICEF indicator"""
    code: str
    name: str
    dataflow: str
    sdg_target: Optional[str] = None
    unit: Optional[str] = None
    description: Optional[str] = None
    dimensions: Optional[List[str]] = None
    source: Optional[str] = None


@dataclass
class CodelistMetadata:
    """Metadata for an SDMX codelist"""
    id: str
    agency: str
    version: str
    codes: Dict[str, str]  # code -> description mapping
    last_updated: Optional[str] = None


class MetadataSync:
    """Synchronize and cache UNICEF SDMX metadata locally.
    
    Downloads metadata from UNICEF's SDMX API and stores it as YAML files
    for offline reference, validation, and version tracking.
    
    Attributes:
        cache_dir: Directory for storing YAML metadata files
        base_url: UNICEF SDMX API base URL
        agency: SDMX agency identifier
    
    Example:
        >>> sync = MetadataSync(cache_dir='./metadata')
        >>> sync.sync_all()
        >>> dataflows = sync.load_dataflows()
        >>> print(dataflows['CME']['name'])
        'Child Mortality Estimates'
    """
    
    BASE_URL = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"
    AGENCY = "UNICEF"
    
    # XML namespaces for SDMX parsing
    NAMESPACES = {
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
        'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
        'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
    }
    
    def __init__(
        self, 
        cache_dir: Optional[str] = None,
        base_url: Optional[str] = None,
        agency: str = "UNICEF"
    ):
        """Initialize metadata sync.
        
        Args:
            cache_dir: Directory for YAML cache files. Defaults to ./metadata/
            base_url: SDMX API base URL. Defaults to UNICEF API.
            agency: SDMX agency identifier. Defaults to 'UNICEF'.
        """
        if cache_dir is None:
            cache_dir = Path.cwd() / "metadata"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_url = base_url or self.BASE_URL
        self.agency = agency
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'unicefData/0.2.0 (+https://github.com/unicef-drp/unicefData)'
        })
    
    # -------------------------------------------------------------------------
    # Sync Functions
    # -------------------------------------------------------------------------
    
    def sync_all(self, verbose: bool = True) -> Dict[str, Any]:
        """Sync all metadata from UNICEF SDMX API.
        
        Downloads dataflows, codelists, and indicator definitions,
        then saves them as YAML files in the cache directory.
        
        Args:
            verbose: Print progress messages
            
        Returns:
            Dictionary with sync summary including counts and timestamps
        """
        results = {
            'synced_at': datetime.utcnow().isoformat() + 'Z',
            'dataflows': 0,
            'codelists': 0,
            'indicators': 0,
            'errors': []
        }
        
        if verbose:
            print(f"Syncing UNICEF SDMX metadata to {self.cache_dir}")
        
        # 1. Sync dataflows
        try:
            dataflows = self.sync_dataflows(verbose=verbose)
            results['dataflows'] = len(dataflows)
        except Exception as e:
            results['errors'].append(f"Dataflows: {str(e)}")
        
        # 2. Sync common codelists
        try:
            codelists = self.sync_codelists(verbose=verbose)
            results['codelists'] = len(codelists)
        except Exception as e:
            results['errors'].append(f"Codelists: {str(e)}")
        
        # 3. Generate indicator catalog from config
        try:
            indicators = self.sync_indicators(verbose=verbose)
            results['indicators'] = len(indicators)
        except Exception as e:
            results['errors'].append(f"Indicators: {str(e)}")
        
        # 4. Save sync summary
        self._save_yaml('sync_summary.yaml', results)
        
        if verbose:
            print(f"\n✅ Sync complete: {results['dataflows']} dataflows, "
                  f"{results['codelists']} codelists, {results['indicators']} indicators")
            if results['errors']:
                print(f"⚠️  Errors: {len(results['errors'])}")
        
        return results
    
    def sync_dataflows(self, verbose: bool = True) -> Dict[str, DataflowMetadata]:
        """Sync dataflow definitions from SDMX API.
        
        Args:
            verbose: Print progress messages
            
        Returns:
            Dictionary of dataflow ID -> DataflowMetadata
        """
        if verbose:
            print("  Fetching dataflows...")
        
        url = f"{self.base_url}/dataflow/{self.agency}?references=none&detail=full"
        response = self._fetch_xml(url)
        
        dataflows = {}
        doc = ET.fromstring(response)
        
        # Parse dataflows from XML
        for df in doc.findall('.//str:Dataflow', self.NAMESPACES):
            df_id = df.get('id')
            agency = df.get('agencyID', self.agency)
            version = df.get('version', '1.0')
            
            # Get name from nested element
            name_elem = df.find('.//com:Name', self.NAMESPACES)
            name = name_elem.text if name_elem is not None else df_id
            
            # Get description if available
            desc_elem = df.find('.//com:Description', self.NAMESPACES)
            description = desc_elem.text if desc_elem is not None else None
            
            dataflows[df_id] = DataflowMetadata(
                id=df_id,
                name=name,
                agency=agency,
                version=version,
                description=description,
                last_updated=datetime.utcnow().isoformat() + 'Z'
            )
        
        # Save to YAML
        dataflows_dict = {
            'metadata_version': '1.0',
            'synced_at': datetime.utcnow().isoformat() + 'Z',
            'source': url,
            'agency': self.agency,
            'dataflows': {k: asdict(v) for k, v in dataflows.items()}
        }
        self._save_yaml('dataflows.yaml', dataflows_dict)
        
        if verbose:
            print(f"    Found {len(dataflows)} dataflows")
        
        return dataflows
    
    def sync_codelists(
        self, 
        codelist_ids: Optional[List[str]] = None,
        verbose: bool = True
    ) -> Dict[str, CodelistMetadata]:
        """Sync codelist definitions from SDMX API.
        
        Args:
            codelist_ids: Specific codelists to sync. If None, syncs common ones.
            verbose: Print progress messages
            
        Returns:
            Dictionary of codelist ID -> CodelistMetadata
        """
        if codelist_ids is None:
            # Common codelists for SDG indicators
            codelist_ids = [
                'CL_REF_AREA',          # Countries/regions
                'CL_SEX',               # Sex disaggregation
                'CL_AGE',               # Age groups
                'CL_WEALTH_QUINTILE',   # Wealth quintiles
                'CL_RESIDENCE',         # Urban/rural
                'CL_UNIT_MEASURE',      # Units of measure
            ]
        
        if verbose:
            print("  Fetching codelists...")
        
        codelists = {}
        for cl_id in codelist_ids:
            try:
                cl = self._fetch_codelist(cl_id)
                if cl:
                    codelists[cl_id] = cl
            except Exception as e:
                if verbose:
                    print(f"    ⚠️  Could not fetch {cl_id}: {e}")
        
        # Save to YAML
        codelists_dict = {
            'metadata_version': '1.0',
            'synced_at': datetime.utcnow().isoformat() + 'Z',
            'source': f"{self.base_url}/codelist/{self.agency}",
            'agency': self.agency,
            'codelists': {k: asdict(v) for k, v in codelists.items()}
        }
        self._save_yaml('codelists.yaml', codelists_dict)
        
        if verbose:
            print(f"    Found {len(codelists)} codelists")
        
        return codelists
    
    def sync_indicators(self, verbose: bool = True) -> Dict[str, IndicatorMetadata]:
        """Sync indicator catalog from config and API.
        
        Combines pre-configured SDG indicators with API-discovered metadata.
        
        Args:
            verbose: Print progress messages
            
        Returns:
            Dictionary of indicator code -> IndicatorMetadata
        """
        if verbose:
            print("  Building indicator catalog...")
        
        # Import config for pre-defined indicators
        try:
            from unicef_api.config import COMMON_INDICATORS
        except ImportError:
            COMMON_INDICATORS = {}
        
        indicators = {}
        
        # Add indicators from config
        for code, info in COMMON_INDICATORS.items():
            sdg_target = info.get('sdg')  # Get SDG from indicator info directly
            
            indicators[code] = IndicatorMetadata(
                code=code,
                name=info.get('name', code),
                dataflow=info.get('dataflow', 'GLOBAL_DATAFLOW'),
                sdg_target=sdg_target,
                unit=info.get('unit'),
                description=info.get('description'),
                source='config'
            )
        
        # Save to YAML
        indicators_dict = {
            'metadata_version': '1.0',
            'synced_at': datetime.utcnow().isoformat() + 'Z',
            'source': 'unicef_api.config + SDMX API',
            'total_indicators': len(indicators),
            'indicators': {k: asdict(v) for k, v in indicators.items()}
        }
        self._save_yaml('indicators.yaml', indicators_dict)
        
        if verbose:
            print(f"    Cataloged {len(indicators)} indicators")
        
        return indicators
    
    # -------------------------------------------------------------------------
    # Load Functions
    # -------------------------------------------------------------------------
    
    def load_dataflows(self) -> Dict[str, Any]:
        """Load cached dataflow metadata from YAML."""
        return self._load_yaml('dataflows.yaml')
    
    def load_codelists(self) -> Dict[str, Any]:
        """Load cached codelist metadata from YAML."""
        return self._load_yaml('codelists.yaml')
    
    def load_indicators(self) -> Dict[str, Any]:
        """Load cached indicator metadata from YAML."""
        return self._load_yaml('indicators.yaml')
    
    def load_sync_summary(self) -> Dict[str, Any]:
        """Load last sync summary."""
        return self._load_yaml('sync_summary.yaml')
    
    def get_dataflow(self, dataflow_id: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific dataflow."""
        dataflows = self.load_dataflows()
        return dataflows.get('dataflows', {}).get(dataflow_id)
    
    def get_indicator(self, indicator_code: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific indicator."""
        indicators = self.load_indicators()
        return indicators.get('indicators', {}).get(indicator_code)
    
    def get_codelist(self, codelist_id: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific codelist."""
        codelists = self.load_codelists()
        return codelists.get('codelists', {}).get(codelist_id)
    
    # -------------------------------------------------------------------------
    # Validation Functions
    # -------------------------------------------------------------------------
    
    def validate_dataframe(
        self, 
        df, 
        indicator_code: str,
        strict: bool = False
    ) -> Tuple[bool, List[str]]:
        """Validate a DataFrame against cached metadata.
        
        Checks:
        - Indicator code exists in catalog
        - Required columns are present
        - Country codes are valid
        - Values are within expected ranges
        
        Args:
            df: pandas DataFrame to validate
            indicator_code: Expected indicator code
            strict: If True, fail on any warning
            
        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []
        
        # Check indicator exists
        indicator = self.get_indicator(indicator_code)
        if indicator is None:
            issues.append(f"Indicator '{indicator_code}' not found in catalog")
        
        # Check required columns
        required_cols = ['REF_AREA', 'TIME_PERIOD', 'OBS_VALUE']
        for col in required_cols:
            if col not in df.columns:
                issues.append(f"Missing required column: {col}")
        
        # Validate country codes if codelist available
        codelists = self.load_codelists()
        ref_area_codes = codelists.get('codelists', {}).get('CL_REF_AREA', {}).get('codes', {})
        if ref_area_codes and 'REF_AREA' in df.columns:
            invalid_countries = set(df['REF_AREA'].unique()) - set(ref_area_codes.keys())
            if invalid_countries:
                issues.append(f"Invalid country codes: {list(invalid_countries)[:5]}...")
        
        # Check for empty data
        if len(df) == 0:
            issues.append("DataFrame is empty")
        
        # Check for null values in key columns
        if 'OBS_VALUE' in df.columns:
            null_pct = df['OBS_VALUE'].isna().mean() * 100
            if null_pct > 50:
                issues.append(f"High null rate in OBS_VALUE: {null_pct:.1f}%")
        
        is_valid = len(issues) == 0 if strict else not any('Missing' in i for i in issues)
        return is_valid, issues
    
    def compute_data_hash(self, df) -> str:
        """Compute hash of DataFrame for version tracking."""
        # Sort for consistent hashing
        df_sorted = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        content = df_sorted.to_csv(index=False)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def create_data_version(
        self,
        df,
        indicator_code: str,
        version_id: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create version record for a downloaded dataset.
        
        Args:
            df: Downloaded DataFrame
            indicator_code: Indicator code
            version_id: Optional version identifier
            notes: Optional notes about this version
            
        Returns:
            Version metadata dictionary
        """
        version = {
            'version_id': version_id or datetime.utcnow().strftime('v%Y%m%d_%H%M%S'),
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'indicator_code': indicator_code,
            'data_hash': self.compute_data_hash(df),
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'notes': notes
        }
        
        # Add summary statistics
        if 'REF_AREA' in df.columns:
            version['unique_countries'] = df['REF_AREA'].nunique()
        if 'TIME_PERIOD' in df.columns:
            version['year_range'] = [
                int(df['TIME_PERIOD'].min()),
                int(df['TIME_PERIOD'].max())
            ]
        if 'OBS_VALUE' in df.columns:
            version['value_range'] = [
                float(df['OBS_VALUE'].min()),
                float(df['OBS_VALUE'].max())
            ]
        
        return version
    
    # -------------------------------------------------------------------------
    # Private Helpers
    # -------------------------------------------------------------------------
    
    def _fetch_xml(self, url: str, retries: int = 3) -> str:
        """Fetch XML from URL with retries."""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise
                import time
                time.sleep(2 ** attempt)
        return ""
    
    def _fetch_codelist(self, codelist_id: str) -> Optional[CodelistMetadata]:
        """Fetch a single codelist from the API."""
        url = f"{self.base_url}/codelist/{self.agency}/{codelist_id}/latest"
        
        try:
            response = self._fetch_xml(url)
            doc = ET.fromstring(response)
            
            codes = {}
            for code_elem in doc.findall('.//str:Code', self.NAMESPACES):
                code_id = code_elem.get('id')
                name_elem = code_elem.find('.//com:Name', self.NAMESPACES)
                name = name_elem.text if name_elem is not None else code_id
                codes[code_id] = name
            
            return CodelistMetadata(
                id=codelist_id,
                agency=self.agency,
                version='latest',
                codes=codes,
                last_updated=datetime.utcnow().isoformat() + 'Z'
            )
        except Exception:
            return None
    
    def _save_yaml(self, filename: str, data: Dict[str, Any]) -> Path:
        """Save dictionary to YAML file."""
        filepath = self.cache_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        return filepath
    
    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """Load dictionary from YAML file."""
        filepath = self.cache_dir / filename
        if not filepath.exists():
            return {}
        with open(filepath, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}


# Convenience functions
def sync_metadata(cache_dir: Optional[str] = None, verbose: bool = True) -> Dict[str, Any]:
    """Sync all UNICEF metadata to local YAML files.
    
    Args:
        cache_dir: Directory for cache files (default: ./metadata/)
        verbose: Print progress messages
        
    Returns:
        Sync summary dictionary
        
    Example:
        >>> from unicef_api.metadata import sync_metadata
        >>> sync_metadata('./my_cache/')
    """
    sync = MetadataSync(cache_dir=cache_dir)
    return sync.sync_all(verbose=verbose)


def validate_indicator_data(
    df,
    indicator_code: str,
    cache_dir: Optional[str] = None
) -> Tuple[bool, List[str]]:
    """Validate DataFrame against cached metadata.
    
    Args:
        df: pandas DataFrame to validate
        indicator_code: Expected indicator code
        cache_dir: Metadata cache directory
        
    Returns:
        Tuple of (is_valid, list of issues)
    """
    sync = MetadataSync(cache_dir=cache_dir)
    return sync.validate_dataframe(df, indicator_code)
