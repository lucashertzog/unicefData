"""
Metadata synchronization, validation, and vintage control for UNICEF SDMX API

This module provides functionality to:
1. Sync dataflow and indicator metadata from the UNICEF SDMX API
2. Cache metadata locally as YAML files for offline use
3. Validate downloaded data against cached metadata
4. Track metadata versions (vintages) for reproducibility and auditing
5. Auto-sync on first use with configurable staleness

Usage:
    >>> from unicef_api.metadata import MetadataSync, sync_metadata
    >>> sync = MetadataSync()
    >>> sync.ensure_synced()  # Auto-syncs if needed
    >>> sync.load_indicators(vintage="2025-12-01")  # Use specific vintage
    >>> sync.compare_vintages("2025-11-01", "2025-12-01")  # Detect changes
"""

import os
import yaml
import shutil
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
    """Synchronize and cache UNICEF SDMX metadata with vintage control.
    
    Downloads metadata from UNICEF's SDMX API and stores it as YAML files
    for offline reference, validation, and version tracking. Supports
    multiple vintages for reproducibility and change detection.
    
    Directory Structure:
        metadata/
        ├── current/              # Latest metadata (symlink or copy)
        │   ├── dataflows.yaml
        │   ├── indicators.yaml
        │   └── codelists.yaml
        ├── vintages/
        │   ├── 2025-12-01/       # Historical snapshots
        │   └── 2025-11-01/
        └── sync_history.yaml     # Log of all syncs
    
    Example:
        >>> sync = MetadataSync(cache_dir='./metadata')
        >>> sync.ensure_synced()  # Auto-sync if needed
        >>> dataflows = sync.load_dataflows()
        >>> dataflows = sync.load_dataflows(vintage="2025-11-01")  # Specific vintage
        >>> changes = sync.compare_vintages("2025-11-01", "2025-12-01")
    """
    
    BASE_URL = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"
    AGENCY = "UNICEF"
    DEFAULT_MAX_AGE_DAYS = 30
    
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
        agency: str = "UNICEF",
        max_age_days: int = 30
    ):
        """Initialize metadata sync with vintage control.
        
        Args:
            cache_dir: Directory for YAML cache files. Defaults to ./metadata/
            base_url: SDMX API base URL. Defaults to UNICEF API.
            agency: SDMX agency identifier. Defaults to 'UNICEF'.
            max_age_days: Days before metadata is considered stale. Default 30.
        """
        if cache_dir is None:
            cache_dir = Path.cwd() / "metadata"
        self.cache_dir = Path(cache_dir)
        self.current_dir = self.cache_dir / "current"
        self.vintages_dir = self.cache_dir / "vintages"
        
        # Create directories
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.current_dir.mkdir(parents=True, exist_ok=True)
        self.vintages_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_url = base_url or self.BASE_URL
        self.agency = agency
        self.max_age_days = max_age_days
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'unicefData/0.2.1 (+https://github.com/unicef-drp/unicefData)'
        })
    
    # -------------------------------------------------------------------------
    # Auto-Sync and Staleness
    # -------------------------------------------------------------------------
    
    def ensure_synced(self, max_age_days: Optional[int] = None, verbose: bool = False) -> bool:
        """Ensure metadata is synced, auto-syncing if stale or missing.
        
        Args:
            max_age_days: Override default staleness threshold
            verbose: Print progress messages
            
        Returns:
            True if sync was performed, False if cache was fresh
        """
        if max_age_days is None:
            max_age_days = self.max_age_days
        
        if self._is_stale(max_age_days):
            if verbose:
                print("Syncing UNICEF metadata (one-time setup or refresh)...")
            self.sync_all(verbose=verbose)
            return True
        return False
    
    def _is_stale(self, max_age_days: int) -> bool:
        """Check if cached metadata is stale or missing."""
        history = self._load_sync_history()
        if not history.get('vintages'):
            return True
        
        latest = history['vintages'][0]
        synced_at = latest.get('synced_at')
        if not synced_at:
            return True
        
        try:
            synced_date = datetime.fromisoformat(synced_at.replace('Z', '+00:00'))
            age = datetime.now(timezone.utc) - synced_date
            return age.days > max_age_days
        except (ValueError, TypeError):
            return True
    
    # -------------------------------------------------------------------------
    # Sync Functions with Vintage Support
    # -------------------------------------------------------------------------
    
    def sync_all(self, verbose: bool = True, create_vintage: bool = True) -> Dict[str, Any]:
        """Sync all metadata from UNICEF SDMX API.
        
        Downloads dataflows, codelists, and indicator definitions,
        saves to current/ and optionally creates a dated vintage snapshot.
        
        Args:
            verbose: Print progress messages
            create_vintage: Save a dated snapshot in vintages/
            
        Returns:
            Dictionary with sync summary including counts and timestamps
        """
        vintage_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        results = {
            'synced_at': datetime.utcnow().isoformat() + 'Z',
            'vintage_date': vintage_date,
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
        
        # 4. Create vintage snapshot
        if create_vintage:
            self._create_vintage(vintage_date, results)
        
        # 5. Update sync history
        self._update_sync_history(results)
        
        if verbose:
            print(f"\n✅ Sync complete: {results['dataflows']} dataflows, "
                  f"{results['codelists']} codelists, {results['indicators']} indicators")
            print(f"   Vintage: {vintage_date}")
            if results['errors']:
                print(f"⚠️  Errors: {len(results['errors'])}")
        
        return results
    
    def sync_dataflows(self, verbose: bool = True) -> Dict[str, DataflowMetadata]:
        """Sync dataflow definitions from SDMX API."""
        if verbose:
            print("  Fetching dataflows...")
        
        url = f"{self.base_url}/dataflow/{self.agency}?references=none&detail=full"
        response = self._fetch_xml(url)
        
        dataflows = {}
        doc = ET.fromstring(response)
        
        for df in doc.findall('.//str:Dataflow', self.NAMESPACES):
            df_id = df.get('id')
            agency = df.get('agencyID', self.agency)
            version = df.get('version', '1.0')
            
            name_elem = df.find('.//com:Name', self.NAMESPACES)
            name = name_elem.text if name_elem is not None else df_id
            
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
        
        # Save to current/
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
        """Sync codelist definitions from SDMX API."""
        if codelist_ids is None:
            codelist_ids = [
                'CL_REF_AREA',
                'CL_SEX',
                'CL_AGE',
                'CL_WEALTH_QUINTILE',
                'CL_RESIDENCE',
                'CL_UNIT_MEASURE',
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
        """Sync indicator catalog from config and API."""
        if verbose:
            print("  Building indicator catalog...")
        
        try:
            from unicef_api.config import COMMON_INDICATORS
        except ImportError:
            COMMON_INDICATORS = {}
        
        indicators = {}
        
        for code, info in COMMON_INDICATORS.items():
            sdg_target = info.get('sdg')
            
            indicators[code] = IndicatorMetadata(
                code=code,
                name=info.get('name', code),
                dataflow=info.get('dataflow', 'GLOBAL_DATAFLOW'),
                sdg_target=sdg_target,
                unit=info.get('unit'),
                description=info.get('description'),
                source='config'
            )
        
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
    # Vintage Management
    # -------------------------------------------------------------------------
    
    def _create_vintage(self, vintage_date: str, results: Dict[str, Any]) -> Path:
        """Create a dated vintage snapshot of current metadata."""
        vintage_path = self.vintages_dir / vintage_date
        
        # If vintage already exists today, skip (don't overwrite)
        if vintage_path.exists():
            return vintage_path
        
        vintage_path.mkdir(parents=True, exist_ok=True)
        
        # Copy current files to vintage
        for filename in ['dataflows.yaml', 'indicators.yaml', 'codelists.yaml']:
            src = self.current_dir / filename
            if src.exists():
                shutil.copy2(src, vintage_path / filename)
        
        # Save vintage summary
        vintage_summary = {
            'vintage_date': vintage_date,
            'synced_at': results['synced_at'],
            'dataflows': results['dataflows'],
            'indicators': results['indicators'],
            'codelists': results['codelists'],
        }
        with open(vintage_path / 'summary.yaml', 'w') as f:
            yaml.dump(vintage_summary, f, default_flow_style=False)
        
        return vintage_path
    
    def list_vintages(self) -> List[str]:
        """List all available vintage dates, newest first.
        
        Returns:
            List of vintage date strings (e.g., ['2025-12-01', '2025-11-01'])
        """
        vintages = []
        if self.vintages_dir.exists():
            for d in self.vintages_dir.iterdir():
                if d.is_dir() and (d / 'dataflows.yaml').exists():
                    vintages.append(d.name)
        return sorted(vintages, reverse=True)
    
    def get_vintage_path(self, vintage: Optional[str] = None) -> Path:
        """Get path to vintage directory.
        
        Args:
            vintage: Date string (e.g., '2025-12-01') or None for current
            
        Returns:
            Path to metadata directory
        """
        if vintage is None:
            return self.current_dir
        
        vintage_path = self.vintages_dir / vintage
        if not vintage_path.exists():
            available = self.list_vintages()
            raise ValueError(
                f"Vintage '{vintage}' not found. Available: {available[:5]}"
            )
        return vintage_path
    
    def compare_vintages(
        self, 
        vintage1: str, 
        vintage2: Optional[str] = None
    ) -> Dict[str, Any]:
        """Compare two vintages to detect changes.
        
        Args:
            vintage1: Earlier vintage date (e.g., '2025-11-01')
            vintage2: Later vintage date (default: current)
            
        Returns:
            Dictionary with added/removed/changed items
        """
        path1 = self.get_vintage_path(vintage1)
        path2 = self.get_vintage_path(vintage2)
        
        changes = {
            'vintage1': vintage1,
            'vintage2': vintage2 or 'current',
            'dataflows': {'added': [], 'removed': [], 'changed': []},
            'indicators': {'added': [], 'removed': [], 'changed': []},
        }
        
        # Compare dataflows
        df1 = self._load_yaml_from_path(path1 / 'dataflows.yaml')
        df2 = self._load_yaml_from_path(path2 / 'dataflows.yaml')
        
        flows1 = set(df1.get('dataflows', {}).keys())
        flows2 = set(df2.get('dataflows', {}).keys())
        
        changes['dataflows']['added'] = list(flows2 - flows1)
        changes['dataflows']['removed'] = list(flows1 - flows2)
        
        # Compare indicators
        ind1 = self._load_yaml_from_path(path1 / 'indicators.yaml')
        ind2 = self._load_yaml_from_path(path2 / 'indicators.yaml')
        
        inds1 = set(ind1.get('indicators', {}).keys())
        inds2 = set(ind2.get('indicators', {}).keys())
        
        changes['indicators']['added'] = list(inds2 - inds1)
        changes['indicators']['removed'] = list(inds1 - inds2)
        
        return changes
    
    # -------------------------------------------------------------------------
    # Load Functions with Vintage Support
    # -------------------------------------------------------------------------
    
    def load_dataflows(self, vintage: Optional[str] = None) -> Dict[str, Any]:
        """Load cached dataflow metadata from YAML.
        
        Args:
            vintage: Vintage date string (e.g., '2025-12-01') or None for current
        """
        path = self.get_vintage_path(vintage)
        return self._load_yaml_from_path(path / 'dataflows.yaml')
    
    def load_codelists(self, vintage: Optional[str] = None) -> Dict[str, Any]:
        """Load cached codelist metadata from YAML."""
        path = self.get_vintage_path(vintage)
        return self._load_yaml_from_path(path / 'codelists.yaml')
    
    def load_indicators(self, vintage: Optional[str] = None) -> Dict[str, Any]:
        """Load cached indicator metadata from YAML."""
        path = self.get_vintage_path(vintage)
        return self._load_yaml_from_path(path / 'indicators.yaml')
    
    def load_sync_summary(self) -> Dict[str, Any]:
        """Load last sync summary (from history)."""
        history = self._load_sync_history()
        if history.get('vintages'):
            return history['vintages'][0]
        return {}
    
    def get_dataflow(self, dataflow_id: str, vintage: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific dataflow."""
        dataflows = self.load_dataflows(vintage=vintage)
        return dataflows.get('dataflows', {}).get(dataflow_id)
    
    def get_indicator(self, indicator_code: str, vintage: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific indicator."""
        indicators = self.load_indicators(vintage=vintage)
        return indicators.get('indicators', {}).get(indicator_code)
    
    def get_codelist(self, codelist_id: str, vintage: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific codelist."""
        codelists = self.load_codelists(vintage=vintage)
        return codelists.get('codelists', {}).get(codelist_id)
    
    # -------------------------------------------------------------------------
    # Validation Functions
    # -------------------------------------------------------------------------
    
    def validate_dataframe(
        self, 
        df, 
        indicator_code: str,
        strict: bool = False,
        vintage: Optional[str] = None
    ) -> Tuple[bool, List[str]]:
        """Validate a DataFrame against cached metadata.
        
        Args:
            df: pandas DataFrame to validate
            indicator_code: Expected indicator code
            strict: If True, fail on any warning
            vintage: Use specific vintage for validation
            
        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []
        
        # Ensure metadata exists
        self.ensure_synced(verbose=False)
        
        # Check indicator exists
        indicator = self.get_indicator(indicator_code, vintage=vintage)
        if indicator is None:
            issues.append(f"Indicator '{indicator_code}' not found in catalog")
        
        # Check required columns
        required_cols = ['REF_AREA', 'TIME_PERIOD', 'OBS_VALUE']
        for col in required_cols:
            if col not in df.columns:
                issues.append(f"Missing required column: {col}")
        
        # Validate country codes if codelist available
        codelists = self.load_codelists(vintage=vintage)
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
        """Create version record for a downloaded dataset."""
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
    # Sync History
    # -------------------------------------------------------------------------
    
    def _load_sync_history(self) -> Dict[str, Any]:
        """Load sync history from YAML."""
        filepath = self.cache_dir / 'sync_history.yaml'
        if not filepath.exists():
            return {'vintages': []}
        with open(filepath, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {'vintages': []}
    
    def _update_sync_history(self, results: Dict[str, Any]) -> None:
        """Update sync history with new sync results."""
        history = self._load_sync_history()
        
        entry = {
            'vintage_date': results.get('vintage_date'),
            'synced_at': results.get('synced_at'),
            'dataflows': results.get('dataflows', 0),
            'indicators': results.get('indicators', 0),
            'codelists': results.get('codelists', 0),
            'errors': results.get('errors', []),
        }
        
        # Add to front of list
        history['vintages'].insert(0, entry)
        
        # Keep only last 50 entries
        history['vintages'] = history['vintages'][:50]
        
        filepath = self.cache_dir / 'sync_history.yaml'
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(history, f, default_flow_style=False, allow_unicode=True)
    
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
        """Save dictionary to YAML file in current/."""
        filepath = self.current_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        return filepath
    
    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """Load dictionary from YAML file in current/."""
        return self._load_yaml_from_path(self.current_dir / filename)
    
    def _load_yaml_from_path(self, filepath: Path) -> Dict[str, Any]:
        """Load dictionary from YAML file at given path."""
        if not filepath.exists():
            return {}
        with open(filepath, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}


# =============================================================================
# Convenience Functions
# =============================================================================

def sync_metadata(
    cache_dir: Optional[str] = None, 
    verbose: bool = True,
    max_age_days: int = 30,
    force: bool = False
) -> Dict[str, Any]:
    """Sync all UNICEF metadata to local YAML files.
    
    Args:
        cache_dir: Directory for cache files (default: ./metadata/)
        verbose: Print progress messages
        max_age_days: Days before auto-refresh (default: 30)
        force: Force sync even if cache is fresh
        
    Returns:
        Sync summary dictionary
        
    Example:
        >>> from unicef_api.metadata import sync_metadata
        >>> sync_metadata('./my_cache/')
        >>> sync_metadata(force=True)  # Force refresh
    """
    sync = MetadataSync(cache_dir=cache_dir, max_age_days=max_age_days)
    
    if force or sync._is_stale(max_age_days):
        return sync.sync_all(verbose=verbose)
    else:
        if verbose:
            print(f"Metadata is fresh (synced within {max_age_days} days). Use force=True to refresh.")
        return sync.load_sync_summary()


def ensure_metadata(cache_dir: Optional[str] = None, max_age_days: int = 30) -> MetadataSync:
    """Ensure metadata exists and is fresh, syncing if needed.
    
    This is the recommended way to get a MetadataSync instance for use.
    
    Args:
        cache_dir: Directory for cache files
        max_age_days: Days before auto-refresh
        
    Returns:
        MetadataSync instance with fresh metadata
    """
    sync = MetadataSync(cache_dir=cache_dir, max_age_days=max_age_days)
    sync.ensure_synced(verbose=False)
    return sync


def validate_indicator_data(
    df,
    indicator_code: str,
    cache_dir: Optional[str] = None,
    vintage: Optional[str] = None
) -> Tuple[bool, List[str]]:
    """Validate DataFrame against cached metadata.
    
    Args:
        df: pandas DataFrame to validate
        indicator_code: Expected indicator code
        cache_dir: Metadata cache directory
        vintage: Use specific vintage for validation
        
    Returns:
        Tuple of (is_valid, list of issues)
    """
    sync = MetadataSync(cache_dir=cache_dir)
    return sync.validate_dataframe(df, indicator_code, vintage=vintage)


def list_vintages(cache_dir: Optional[str] = None) -> List[str]:
    """List all available metadata vintages.
    
    Args:
        cache_dir: Metadata cache directory
        
    Returns:
        List of vintage date strings, newest first
    """
    sync = MetadataSync(cache_dir=cache_dir)
    return sync.list_vintages()


def compare_vintages(
    vintage1: str,
    vintage2: Optional[str] = None,
    cache_dir: Optional[str] = None
) -> Dict[str, Any]:
    """Compare two metadata vintages to detect changes.
    
    Args:
        vintage1: Earlier vintage date
        vintage2: Later vintage date (default: current)
        cache_dir: Metadata cache directory
        
    Returns:
        Dictionary with added/removed/changed items
    """
    sync = MetadataSync(cache_dir=cache_dir)
    return sync.compare_vintages(vintage1, vintage2)
