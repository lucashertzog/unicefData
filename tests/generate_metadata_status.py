#!/usr/bin/env python3
"""
Generate metadata status summary table for unicefData package.

This script checks the metadata files across all three platforms (Python, R, Stata)
and generates a markdown table showing the status of each file.

Usage:
    python generate_metadata_status.py
    python generate_metadata_status.py --output markdown
    python generate_metadata_status.py --output csv
    python generate_metadata_status.py --detailed
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional, NamedTuple
import yaml
import argparse

# Repository root (relative to this script location)
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent

# Metadata directories
METADATA_DIRS = {
    'Python': REPO_ROOT / 'python' / 'metadata' / 'current',
    'R': REPO_ROOT / 'R' / 'metadata' / 'current',
    'Stata (Python)': REPO_ROOT / 'stata' / 'metadata' / 'current',
    'Stata (only)': REPO_ROOT / 'stata' / 'metadata' / 'current',
}

# Files to check (base names without suffix)
METADATA_FILES = [
    '_unicefdata_dataflows',
    '_unicefdata_codelists', 
    '_unicefdata_countries',
    '_unicefdata_regions',
    '_unicefdata_indicators',
    'unicef_indicators_metadata',
    'dataflow_index',
    'dataflows/*.yaml',  # Special case for directory
]

# Expected metadata header keys
EXPECTED_HEADER_KEYS = ['metadata', '_metadata', 'version', 'synced_at', 'source', 'agency']


class FileStats(NamedTuple):
    """Statistics for a metadata file."""
    exists: bool
    records: Optional[int]
    lines: Optional[int]
    has_header: bool
    header_keys: List[str]


def count_lines(filepath: Path) -> int:
    """Count the number of lines in a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def check_header(data: dict) -> Tuple[bool, List[str]]:
    """
    Check if a YAML file has a proper metadata header.
    
    Returns:
        Tuple of (has_header, list of header keys found)
    """
    if not isinstance(data, dict):
        return (False, [])
    
    found_keys = []
    
    # Check for metadata block
    if 'metadata' in data:
        found_keys.append('metadata')
        if isinstance(data['metadata'], dict):
            found_keys.extend([f"metadata.{k}" for k in data['metadata'].keys()])
    
    if '_metadata' in data:
        found_keys.append('_metadata')
        if isinstance(data['_metadata'], dict):
            found_keys.extend([f"_metadata.{k}" for k in data['_metadata'].keys()])
    
    # Check for top-level header keys
    for key in ['version', 'synced_at', 'source', 'agency', 'platform', 
                'metadata_version', 'last_updated', 'content_type']:
        if key in data:
            found_keys.append(key)
    
    has_header = len(found_keys) > 0
    return (has_header, found_keys)


def count_records_in_yaml(filepath: Path) -> Tuple[Optional[int], bool, List[str]]:
    """
    Count the number of records in a YAML file and check for header.
    
    Returns:
        Tuple of (record_count, has_header, header_keys)
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if data is None:
            return (0, False, [])
        
        has_header, header_keys = check_header(data)
        
        # Handle different file structures
        if isinstance(data, dict):
            # Check common list keys
            for key in ['dataflows', 'codelists', 'countries', 'regions', 'indicators', 'codes']:
                if key in data:
                    if isinstance(data[key], list):
                        return (len(data[key]), has_header, header_keys)
                    elif isinstance(data[key], dict):
                        return (len(data[key]), has_header, header_keys)
            
            # For dataflow_index, count dataflows list
            if 'dataflows' in data:
                return (len(data['dataflows']), has_header, header_keys)
            
            # For indicator metadata with 'indicators' as dict
            if 'indicators' in data and isinstance(data['indicators'], dict):
                return (len(data['indicators']), has_header, header_keys)
                
            # Fallback: count top-level keys (excluding metadata)
            count = len([k for k in data.keys() if not k.startswith('_') and k != 'metadata'])
            return (count, has_header, header_keys)
        elif isinstance(data, list):
            return (len(data), has_header, header_keys)
        
        return (None, has_header, header_keys)
    except Exception as e:
        return (None, False, [])


def get_file_stats(base_dir: Path, file_base: str, is_stataonly: bool = False) -> FileStats:
    """
    Get comprehensive statistics for a file.
    
    Returns:
        FileStats named tuple
    """
    # Handle special case for dataflows directory
    if file_base == 'dataflows/*.yaml':
        if is_stataonly:
            dataflows_dir = base_dir / 'dataflows_stataonly'
        else:
            dataflows_dir = base_dir / 'dataflows'
        
        if not dataflows_dir.exists():
            return FileStats(exists=False, records=None, lines=None, has_header=False, header_keys=[])
        
        yaml_files = list(dataflows_dir.glob('*.yaml'))
        count = len(yaml_files)
        
        # Count total lines across all files
        total_lines = sum(count_lines(f) for f in yaml_files)
        
        # Check header on first file
        has_header = False
        header_keys = []
        if yaml_files:
            _, has_header, header_keys = count_records_in_yaml(yaml_files[0])
        
        return FileStats(
            exists=count > 0,
            records=count if count > 0 else None,
            lines=total_lines if count > 0 else None,
            has_header=has_header,
            header_keys=header_keys
        )
    
    # Regular files
    if is_stataonly:
        filename = f"{file_base}_stataonly.yaml"
    else:
        filename = f"{file_base}.yaml"
    
    filepath = base_dir / filename
    
    if not filepath.exists():
        return FileStats(exists=False, records=None, lines=None, has_header=False, header_keys=[])
    
    lines = count_lines(filepath)
    records, has_header, header_keys = count_records_in_yaml(filepath)
    
    return FileStats(
        exists=True,
        records=records,
        lines=lines,
        has_header=has_header,
        header_keys=header_keys
    )


def generate_status_table(detailed: bool = False) -> List[Dict]:
    """Generate the status table data."""
    results = []
    
    for file_base in METADATA_FILES:
        row = {'file': f"`{file_base}.yaml`" if file_base != 'dataflows/*.yaml' else '`dataflows/*.yaml`'}
        
        for platform, base_dir in METADATA_DIRS.items():
            is_stataonly = platform == 'Stata (only)'
            stats = get_file_stats(base_dir, file_base, is_stataonly)
            
            if detailed:
                if stats.exists and stats.records is not None:
                    header_mark = "📋" if stats.has_header else "⚠️"
                    row[platform] = f"✓ ({stats.records}) [{stats.lines}L] {header_mark}"
                elif stats.exists:
                    header_mark = "📋" if stats.has_header else "⚠️"
                    row[platform] = f"✓ [{stats.lines}L] {header_mark}"
                else:
                    row[platform] = "✗"
                
                # Store detailed info for later
                row[f"{platform}_stats"] = stats
            else:
                if stats.exists and stats.records is not None:
                    row[platform] = f"✓ ({stats.records})"
                elif stats.exists:
                    row[platform] = "✓"
                else:
                    row[platform] = "✗"
        
        results.append(row)
    
    return results


def format_markdown_table(data: List[Dict], detailed: bool = False) -> str:
    """Format the data as a markdown table."""
    platforms = ['Python', 'R', 'Stata (Python)', 'Stata (only)']
    
    # Header
    lines = [
        "### Metadata File Status Summary",
        "",
    ]
    
    if detailed:
        lines.append("| File | " + " | ".join(platforms) + " |")
        lines.append("|------|" + "|".join(["------" for _ in platforms]) + "|")
    else:
        lines.append("| File | " + " | ".join(platforms) + " |")
        lines.append("|------|" + "|".join(["------" for _ in platforms]) + "|")
    
    # Data rows
    for row in data:
        cells = [row['file']] + [row.get(p, "✗") for p in platforms]
        lines.append("| " + " | ".join(cells) + " |")
    
    # Legend
    lines.extend([
        "",
        "**Legend:**",
        "- **Python**: Files in `python/metadata/current/`",
        "- **R**: Files in `R/metadata/current/`",
        "- **Stata (Python)**: Standard files in `stata/metadata/current/` (generated with Python assistance)",
        "- **Stata (only)**: Files with `_stataonly` suffix in `stata/metadata/current/` (pure Stata parser)",
    ])
    
    if detailed:
        lines.extend([
            "",
            "**Format:** `✓ (records) [lines] header_status`",
            "- 📋 = Has metadata header",
            "- ⚠️ = Missing metadata header",
        ])
    
    return "\n".join(lines)


def format_detailed_report(data: List[Dict]) -> str:
    """Generate a detailed report with all statistics."""
    platforms = ['Python', 'R', 'Stata (Python)', 'Stata (only)']
    
    lines = [
        "## Detailed Metadata File Report",
        "",
        f"Generated: {__import__('datetime').datetime.now().isoformat()}",
        "",
    ]
    
    for row in data:
        lines.append(f"### {row['file']}")
        lines.append("")
        lines.append("| Platform | Exists | Records | Lines | Header | Header Keys |")
        lines.append("|----------|--------|---------|-------|--------|-------------|")
        
        for platform in platforms:
            stats_key = f"{platform}_stats"
            if stats_key in row:
                stats = row[stats_key]
                exists = "✓" if stats.exists else "✗"
                records = str(stats.records) if stats.records is not None else "-"
                file_lines = str(stats.lines) if stats.lines is not None else "-"
                header = "✓" if stats.has_header else "✗"
                header_keys = ", ".join(stats.header_keys[:3]) if stats.header_keys else "-"
                if len(stats.header_keys) > 3:
                    header_keys += "..."
                
                lines.append(f"| {platform} | {exists} | {records} | {file_lines} | {header} | {header_keys} |")
        
        lines.append("")
    
    return "\n".join(lines)


def format_csv(data: List[Dict], detailed: bool = False) -> str:
    """Format the data as CSV."""
    platforms = ['Python', 'R', 'Stata (Python)', 'Stata (only)']
    
    if detailed:
        headers = ["File"]
        for p in platforms:
            headers.extend([f"{p}_exists", f"{p}_records", f"{p}_lines", f"{p}_has_header"])
        lines = [",".join(headers)]
        
        for row in data:
            cells = [row['file'].replace('`', '')]
            for p in platforms:
                stats_key = f"{p}_stats"
                if stats_key in row:
                    stats = row[stats_key]
                    cells.extend([
                        str(stats.exists),
                        str(stats.records) if stats.records else "",
                        str(stats.lines) if stats.lines else "",
                        str(stats.has_header)
                    ])
                else:
                    cells.extend(["False", "", "", "False"])
            lines.append(",".join(cells))
    else:
        lines = ["File," + ",".join(platforms)]
        for row in data:
            cells = [row['file'].replace('`', '')] + [row.get(p, "✗") for p in platforms]
            lines.append(",".join(cells))
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Generate metadata status summary table')
    parser.add_argument('--output', '-o', choices=['markdown', 'csv', 'both', 'report'], 
                        default='markdown', help='Output format')
    parser.add_argument('--detailed', '-d', action='store_true',
                        help='Include line counts and header status')
    parser.add_argument('--save', '-s', action='store_true',
                        help='Save output to file(s)')
    args = parser.parse_args()
    
    print("Scanning metadata directories...")
    print(f"  Repository root: {REPO_ROOT}")
    print()
    
    # Check directories exist
    for platform, path in METADATA_DIRS.items():
        exists = "✓" if path.exists() else "✗"
        print(f"  {platform}: {path} [{exists}]")
    print()
    
    # Generate table
    data = generate_status_table(detailed=args.detailed or args.output == 'report')
    
    if args.output == 'report':
        report = format_detailed_report(data)
        print(report)
        
        if args.save:
            output_path = SCRIPT_DIR / 'metadata_status_report.md'
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"\nSaved to: {output_path}")
    
    elif args.output in ['markdown', 'both']:
        md_output = format_markdown_table(data, detailed=args.detailed)
        print(md_output)
        
        if args.save:
            output_path = SCRIPT_DIR / 'metadata_status.md'
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(md_output)
            print(f"\nSaved to: {output_path}")
    
    if args.output in ['csv', 'both']:
        csv_output = format_csv(data, detailed=args.detailed)
        if args.output == 'both':
            print("\n--- CSV Format ---\n")
        print(csv_output)
        
        if args.save:
            output_path = SCRIPT_DIR / 'metadata_status.csv'
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(csv_output)
            print(f"\nSaved to: {output_path}")


if __name__ == '__main__':
    main()
