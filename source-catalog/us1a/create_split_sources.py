#!/usr/bin/env python3
"""
Create sibling source directories organized by lon/lat grid with de-duplication.

Structure: us1ab/, us1ac/, us1ad/, etc. (sequential siblings to us1aa/)
- Longitude-first grouping (keeps same UTM zone together)
- Configurable grouping sizes for lat/lon bands
- De-duplicates files by primary UTM zone for each longitude
- Deterministic directory naming based on sorted grid cells

Usage:
    python create_nested_sources.py [--dry-run] [--lon N] [--lat N]

Examples:
    # Normal run with defaults
    python create_nested_sources.py

    # Dry run to preview metrics without creating directories
    python create_nested_sources.py --dry-run

    # Override grouping sizes
    python create_nested_sources.py --lon 6 --lat 2

    # Dry run with custom grouping
    python create_nested_sources.py --dry-run --lon 3 --lat 1
"""

import json
import argparse
from pathlib import Path
import shutil
from grid_utils import (
    deduplicate_files,
    get_friendly_location,
    generate_directory_suffix,
    sort_lat_lon_bands,
)

# Default Configuration
DEFAULT_LON_BAND_GROUPING = (
    6  # Must be a divisor of 6 (1, 2, 3, or 6) to align with UTM zones
)
DEFAULT_LAT_BAND_GROUPING = 90  # Any integer value for latitude grouping

if 6 % DEFAULT_LON_BAND_GROUPING != 0:
    raise ValueError(
        f'DEFAULT_LON_BAND_GROUPING must be a divisor of 6 (1, 2, 3, or 6), got {DEFAULT_LON_BAND_GROUPING}'
    )


def print_divider(character='='):
    print(character * 80)


def generate_justfile_content(source_name):
    return f"""# Source preparation pipeline to be run from mapterhorn/pipelines folder

[no-cd]
default:
    uv run python source_download.py {source_name}
    uv run python source_unzip.py {source_name}
    uv run python source_verify_crs.py {source_name}
    uv run python source_slice.py {source_name} 16384
    uv run python source_to_cog.py {source_name}
    uv run python source_bounds.py {source_name}
    uv run python source_polygonize.py {source_name} 32
    uv run python source_create_tarball.py {source_name}"""


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Create nested source directories organized by lon/lat grid with de-duplication.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Normal run with defaults
  python create_nested_sources.py
  
  # Dry run to preview metrics without creating directories
  python create_nested_sources.py --dry-run
  
  # Override grouping sizes
  python create_nested_sources.py --lon 6 --lat 2
  
  # Dry run with custom grouping
  python create_nested_sources.py --dry-run --lon 3 --lat 1
        """,
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview metrics without creating directories or files',
    )
    parser.add_argument(
        '--lon',
        type=int,
        default=DEFAULT_LON_BAND_GROUPING,
        metavar='N',
        help=f'Longitude grouping in degrees (must be divisor of 6: 1, 2, 3, or 6). Default: {DEFAULT_LON_BAND_GROUPING}',
    )
    parser.add_argument(
        '--lat',
        type=int,
        default=DEFAULT_LAT_BAND_GROUPING,
        metavar='N',
        help=f'Latitude grouping in degrees (any positive integer). Default: {DEFAULT_LAT_BAND_GROUPING}',
    )

    args = parser.parse_args()

    if 6 % args.lon != 0:
        parser.error(f'--lon must be a divisor of 6 (1, 2, 3, or 6), got {args.lon}')

    if args.lat < 1:
        parser.error(f'--lat must be a positive integer, got {args.lat}')

    LON_BAND_GROUPING = args.lon
    LAT_BAND_GROUPING = args.lat
    DRY_RUN = args.dry_run

    script_dir = Path(__file__).parent

    # Read CRS/lon/lat-organized file data
    grid_file = script_dir / 'files_by_crs_lon_lat.json'
    if not grid_file.exists():
        print(f'Error: {grid_file} not found!')
        print('Run analyze_file_breakdown.py first.')
        return

    with open(grid_file) as f:
        crs_data = json.load(f)

    if DRY_RUN:
        print_divider()
        print('DRY RUN MODE - No directories will be created')
        print_divider()

    print('\nAnalyzing sibling source directories with de-duplication...\n')
    print('Configuration:')
    print(f'  Longitude grouping: {LON_BAND_GROUPING}° bands (UTM-zone aligned)')
    print(f'  Latitude grouping: {LAT_BAND_GROUPING}° bands')
    print(f'  Structure: us1a/, us1a/, us1a/, etc. (siblings to {script_dir.name}/)')
    print('  Longitude bands aligned to UTM boundaries (no mixed CRS)')
    print(
        f'  Mode: {"DRY RUN (preview only)" if DRY_RUN else "LIVE (creating directories)"}\n'
    )

    # Group files by lon/lat grid (with grouping and de-duplication)
    grid_data = deduplicate_files(crs_data, LON_BAND_GROUPING, LAT_BAND_GROUPING)

    total_sources = 0
    total_files = 0
    total_bytes = 0
    mixed_crs_directories = []
    all_directories = []

    # Get base directory name
    base_dir_name = script_dir.name
    base_prefix = base_dir_name[:-1]  # Remove last letter (e.g., 'us1a' -> 'us1')
    parent_dir = script_dir.parent

    # Sort grid cells deterministically (by lon_group then lat_group)
    sorted_grid_keys = sorted(grid_data.keys())

    # Build sorted lon/lat band to index mappings
    unique_lon_bands = sort_lat_lon_bands(
        set(data['lon_group'] for data in grid_data.values())
    )

    lon_to_index = {lon: idx for idx, lon in enumerate(unique_lon_bands)}

    # Build lat_to_index per longitude band
    lon_lat_to_index = {}
    for lon_band in unique_lon_bands:
        lat_bands_in_lon = sort_lat_lon_bands(
            set(
                data['lat_group']
                for _key, data in grid_data.items()
                if data['lon_group'] == lon_band
            )
        )
        lon_lat_to_index[lon_band] = {
            lat: idx for idx, lat in enumerate(lat_bands_in_lon)
        }

    for grid_key in sorted_grid_keys:
        data = grid_data[grid_key]
        lon_group = data['lon_group']
        lat_group = data['lat_group']

        # Generate directory name based on lon band indices
        lon_index = lon_to_index[lon_group]
        dir_suffix = generate_directory_suffix(lon_index)
        dir_name = f'{base_prefix}{dir_suffix}'
        source_dir = parent_dir / dir_name

        if not DRY_RUN:
            source_dir.mkdir(parents=True, exist_ok=True)

        file_list_path = source_dir / 'file_list.txt'
        unique_files = sorted(set(data['files']))
        if not DRY_RUN:
            with open(file_list_path, 'w') as f:
                f.write('\n'.join(unique_files))

        # Get location for display and metadata
        location = get_friendly_location(
            lon_group, lat_group, LON_BAND_GROUPING, LAT_BAND_GROUPING
        )

        # Copy/generate files for directory (skip if same directory)
        if source_dir != script_dir and not DRY_RUN:
            metadata_src = script_dir / 'metadata.json'
            metadata_dst = source_dir / 'metadata.json'
            if metadata_src.exists():
                shutil.copy2(metadata_src, metadata_dst)

            license_src = script_dir / 'LICENSE.pdf'
            license_dst = source_dir / 'LICENSE.pdf'
            if license_src.exists():
                shutil.copy2(license_src, license_dst)

        # Generate Justfile with hardcoded source name
        if not DRY_RUN:
            justfile_dst = source_dir / 'Justfile'
            justfile_content = generate_justfile_content(dir_name)
            with open(justfile_dst, 'w') as f:
                f.write(justfile_content)

        file_count = len(unique_files)
        size_gib = data['bytes'] / (1024**3)
        crs_list = sorted(data['crs_codes'])

        dir_info = {
            'path': dir_name,
            'grid_location': f'{lon_group}/{lat_group}',
            'location': location,
            'crs_list': crs_list,
            'file_count': file_count,
            'size_gib': size_gib,
        }
        all_directories.append(dir_info)

        # Check for mixed CRS
        if len(crs_list) > 1:
            mixed_crs_directories.append(dir_info)

        action_verb = 'Analyzed' if DRY_RUN else 'Created'
        print(f'✓ {action_verb} {dir_name}/')
        print(f'    Grid: {lon_group}/{lat_group}')
        print(f'    Location: {location}')
        print(f'    CRS: {", ".join(crs_list)}')
        print(f'    Files: {file_count}')
        print(f'    Size: {size_gib:.2f} GiB')
        print()

        total_sources += 1
        total_files += file_count
        total_bytes += data['bytes']

    total_gib = total_bytes / (1024**3)
    total_tib = total_bytes / (1024**4)

    print_divider()
    print('SUMMARY')
    print_divider()
    print(f'Mode: {"DRY RUN" if DRY_RUN else "LIVE"}')
    print(f'Grid grouping: {LON_BAND_GROUPING}° lon, {LAT_BAND_GROUPING}° lat')
    print(f'Total directories {"analyzed" if DRY_RUN else "created"}: {total_sources}')
    print(f'Total files: {total_files:,}')
    print(f'Total size: {total_gib:.2f} GiB ({total_tib:.2f} TiB)')
    print(
        f'Longitude bands: {len(unique_lon_bands)} (letters a-{chr(ord("a") + len(unique_lon_bands) - 1)})'
    )
    print(
        f'Max latitude bands per longitude: {max(len(lon_lat_to_index[lon]) for lon in unique_lon_bands)}'
    )
    if not DRY_RUN:
        print(f'\nSibling directories created in: {parent_dir}/')
    else:
        print(f'\nSibling directories would be created in: {parent_dir}/')
        print('(Run without --dry-run to actually create directories)')

    if mixed_crs_directories:
        print(
            f'\nWARNING: {len(mixed_crs_directories)} directories contain multiple CRS:'
        )
        print_divider()
        for item in mixed_crs_directories:
            print(f'  {item["path"]}/')
            print(f'    CRS: {", ".join(item["crs_list"])}')
            print(
                f'    Files: {item["file_count"]:,}, Size: {item["size_gib"]:.2f} GiB'
            )
        print_divider()
        print('This indicates a potential issue with UTM zone alignment.')
        print('Expected: Each directory should contain only one CRS.')
    else:
        print('\n✓ All directories contain single CRS')


if __name__ == '__main__':
    main()
