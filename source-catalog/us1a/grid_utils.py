#!/usr/bin/env python3
"""
Shared utilities for lon/lat grid operations, UTM zone alignment, and deduplication.
"""

import math


def parse_lon_band(lon_band):
    """Parse longitude band string to degrees (e.g., 'w074' -> -74)"""
    direction = lon_band[0]
    value = int(lon_band[1:])
    return -value if direction == 'w' else value


def parse_lat_band(lat_band):
    """Parse latitude band string to degrees (e.g., 'n40' -> 40)"""
    direction = lat_band[0]
    value = int(lat_band[1:])
    return value if direction == 'n' else -value


def parse_band_number(band_str):
    """
    Parse band string to numeric value for sorting.

    Returns values such that sorting in ascending order gives:
    - Longitude: west to east (w180 -> w001 -> e000 -> e180)
    - Latitude: south to north (s90 -> s01 -> n00 -> n90)
    """
    if band_str.startswith('n'):
        # North latitudes: positive values
        return int(band_str[1:])
    elif band_str.startswith('s'):
        # South latitudes: negative values
        return -int(band_str[1:])
    elif band_str.startswith('w'):
        # West longitudes: negative values
        return -int(band_str[1:])
    elif band_str.startswith('e'):
        # East longitudes: positive values
        return int(band_str[1:])
    return 0


def sort_lat_lon_bands(bands):
    """
    Sort longitude bands from west to east (increasing values), or latitude bands from south to north (increasing values).

    Args:
        bands: Iterable of longitude band strings (e.g., ['w156', 'w150', 'e000']), or latitude band strings (e.g., ['n40', 's10', 'n20'])

    Returns:
        List of sorted longitude bands (west to east), or latitude bands (south to north)
    """
    return sorted(bands, key=parse_band_number)


def get_primary_utm_zone(lon_deg):
    """
    Determine primary UTM zone for a given longitude.
    Returns the EPSG code pattern (e.g., EPSG:26918 for UTM 18N)

    UTM zones:
    Zone 1: 180°W to 174°W (-180° to -174°)
    Zone 2: 174°W to 168°W (-174° to -168°)
    Zone N: Each zone is 6° wide
    """
    zone = math.floor((lon_deg + 180) / 6) + 1
    epsg_code = f'EPSG:269{zone:02d}'
    return epsg_code


def get_utm_aligned_lon_group(lon_deg, grouping_size):
    """
    Get UTM-aligned longitude group name.
    Groups are aligned to UTM zone boundaries (which are 6° wide).

    Examples with grouping_size=6:
    - -156°W is in UTM Zone 5 (boundary at -156°W), returns w156
    - -75°W is in UTM Zone 18 (boundary at -78°W), returns w078

    Examples with grouping_size=2:
    - -156°W -> UTM Zone 5 boundary is -156°W, subdivided as: w156, w158, w160
    - -75°W -> UTM Zone 18 boundary is -78°W, subdivided as: w078, w076, w074
    """
    zone = math.floor((lon_deg + 180) / 6) + 1
    west_boundary = -180 + (zone - 1) * 6
    offset_in_zone = lon_deg - west_boundary
    group_offset = (offset_in_zone // grouping_size) * grouping_size
    group_boundary = west_boundary + group_offset

    if group_boundary >= 0:
        return f'e{abs(group_boundary):03d}'
    else:
        return f'w{abs(group_boundary):03d}'


def group_band_name(band_value, is_longitude, grouping_size, offset=0):
    """
    Generate grouped band name with optional offset.
    For grouping_size=2: values -74,-75 -> w074; -76,-77 -> w076
    band_value should be signed (negative for W/S, positive for E/N)
    """
    grouped_value = (
        math.floor((band_value - offset) / grouping_size) * grouping_size + offset
    )

    if is_longitude:
        if grouped_value >= 0:
            return f'e{abs(grouped_value):03d}'
        else:
            return f'w{abs(grouped_value):03d}'
    else:
        if grouped_value >= 0:
            return f'n{abs(grouped_value):02d}'
        else:
            return f's{abs(grouped_value):02d}'


def deduplicate_files(
    crs_data, lon_grouping=1, lat_grouping=1, lon_offset=0, lat_offset=0
):
    """
    Process CRS data to deduplicate files across UTM zones and group by grid cells.

    Args:
        crs_data: Dictionary from files_by_crs_lon_lat.json
        lon_grouping: Longitude grouping size in degrees
        lat_grouping: Latitude grouping size in degrees
        lon_offset: Longitude grid offset in degrees
        lat_offset: Latitude grid offset in degrees

    Returns:
        Dictionary mapping grid_key (lon/lat) to cell data with deduplicated files
    """
    file_metadata = []

    for epsg_code, epsg_data in crs_data.items():
        for lon_band, lon_data in epsg_data.get('lon_bands', {}).items():
            lon_deg = parse_lon_band(lon_band)

            for lat_band, lat_data in lon_data.get('lat_bands', {}).items():
                lat_deg = parse_lat_band(lat_band)

                for file_url in lat_data['files']:
                    file_metadata.append(
                        {
                            'url': file_url,
                            'lon_deg': lon_deg,
                            'lat_deg': lat_deg,
                            'epsg_code': epsg_code,
                            'size': lat_data['bytes'] / len(lat_data['files']),
                        }
                    )

    grid_data = {}
    seen_files = set()

    for file_info in file_metadata:
        url = file_info['url']
        lon_deg = file_info['lon_deg']
        lat_deg = file_info['lat_deg']
        epsg_code = file_info['epsg_code']

        if url in seen_files:
            continue

        primary_epsg = get_primary_utm_zone(lon_deg)
        if epsg_code != primary_epsg:
            continue

        seen_files.add(url)

        lon_group = group_band_name(lon_deg, True, lon_grouping, lon_offset)
        lat_group = group_band_name(lat_deg, False, lat_grouping, lat_offset)
        grid_key = f'{lon_group}/{lat_group}'

        if grid_key not in grid_data:
            grid_data[grid_key] = {
                'files': [],
                'file_count': 0,
                'bytes': 0,
                'lon_group': lon_group,
                'lat_group': lat_group,
                'crs_codes': set(),
            }

        grid_data[grid_key]['files'].append(url)
        grid_data[grid_key]['file_count'] += 1
        grid_data[grid_key]['bytes'] += file_info['size']
        grid_data[grid_key]['crs_codes'].add(epsg_code)

    return grid_data


def format_bytes(bytes_val):
    """Format bytes as GiB with byte count"""
    gib = bytes_val / (1024**3)
    return f'{gib:.2f} GiB ({bytes_val:,} bytes)'


def get_friendly_location(lon_group, lat_group, lon_grouping, lat_grouping):
    """Create human-readable location description"""
    lon_deg_abs = abs(int(lon_group[1:]))
    lat_deg_signed = parse_lat_band(lat_group)
    lon_dir = 'W' if lon_group.startswith('w') else 'E'

    lon_end = lon_deg_abs + lon_grouping

    # Handle latitude range with proper clamping
    lat_start = max(lat_deg_signed, -90)
    lat_end = min(lat_deg_signed + lat_grouping, 90)

    lon_range = f'{lon_deg_abs}-{lon_end}°{lon_dir}'

    # Special case: grouping >= 180 spans entire globe
    if lat_grouping >= 180 or (lat_start == -90 and lat_end == 90):
        lat_range = '90°S-90°N'
    elif lat_start < 0 and lat_end > 0:
        lat_range = f'{abs(lat_start)}°S-{lat_end}°N'
    elif lat_start < 0:
        lat_range = f'{abs(lat_end)}-{abs(lat_start)}°S'
    else:
        lat_range = f'{lat_start}-{lat_end}°N'

    return f'{lat_range}, {lon_range}'


def generate_directory_suffix(lon_index, lat_index=None):
    """
    Generate two-letter suffix for directory naming based on lon/lat band indices.

    First letter represents the longitude band (a, b, c, ...)
    Second letter represents the latitude band within that longitude (a, b, c, ...)

    Args:
        lon_index: Zero-based longitude band index (0 -> 'a', 1 -> 'b', ...)
        lat_index: Zero-based latitude band index within the longitude (0 -> 'a', 1 -> 'b', ...)

    Returns:
        str: Two-letter suffix (e.g., 'aa', 'ab', 'ba', 'bb', ...)

    Examples:
        lon_index=0, lat_index=0 -> 'aa'
        lon_index=0, lat_index=1 -> 'ab'
        lon_index=1, lat_index=0 -> 'ba'
        lon_index=25, lat_index=25 -> 'zz'
    """
    if lon_index > 25 or lat_index is not None and lat_index > 25:
        raise ValueError(
            f'Index too large: lon_index={lon_index}, lat_index={lat_index}. Max supported is 25 (26 bands per dimension)'
        )

    first_letter = chr(ord('a') + lon_index)
    if lat_index is None:
        return first_letter

    second_letter = chr(ord('a') + lat_index)
    return first_letter + second_letter
