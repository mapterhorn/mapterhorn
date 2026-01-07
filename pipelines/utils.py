"""Shared utilities for DEM pipeline: I/O, tile math, Terrarium encoding, COG ops.

This module provides core infrastructure used across all pipeline stages:

**Global constants**:
- macrotile_z = 12: Fixed zoom level for atomic coverage units (~150km tiles)
- macrotile_buffer_3857 = 150: Buffer zone in meters (enables seamless blending)
- num_overviews = 6: Pyramid levels for downsampling
- X_MIN_3857, X_MAX_3857: Web Mercator bounds (±20037508m at equator)

**I/O helpers**:
- run_command: Execute shell commands with stdout/stderr capture
- create_folder: mkdir -p equivalent for Python
- HashWriter: Wrapper computing SHA256 during file writes

**Tile math**:
- get_aggregation_ids: List ULID-named aggregation batches chronologically
- get_dirty_aggregation_filenames: Identify changed aggregations for incremental rebuilds
- get_vertical_rounding_multiplier: Compute zoom-appropriate elevation precision
- get_pmtiles_folder: Generate hierarchical storage paths (e.g., pmtiles-store/42/)

**Terrarium encoding**:
- save_terrarium_tile: Convert float elevation → RGB WebP with zoom-based rounding
  * Formula: elevation(m) = R×256 + G + B/256 - 32768
  * Range: -32768m to +32767.99m with up to 1/256m (~4mm) precision
  * Dynamic rounding: Coarser at low zooms to match pixel resolution

**PMTiles packaging**:
- create_archive: Bundle WebP tiles into single-file PMTiles archives
  * Computes bbox and zoom metadata from tile coordinates
  * Sorts by tile_id for efficient sequential access
  * Enables HTTP range requests from static storage
"""

import subprocess
from pathlib import Path
from glob import glob
import math
import os
import hashlib

import numpy as np

from rasterio.warp import transform_bounds
import mercantile
import imagecodecs
from pmtiles.tile import zxy_to_tileid, tileid_to_zxy, TileType, Compression
from pmtiles.writer import Writer

macrotile_z = 12
macrotile_buffer_3857 = 150
num_overviews = 6

X_MIN_3857, _, X_MAX_3857, __ = transform_bounds(
    "EPSG:4326", "EPSG:3857", -180, 0, 180, 0
)


def run_command(command, silent=True):
    """Run a shell command and return stdout/err strings.

    Execution details:
    1. Spawns subprocess with shell=True (enables piping, redirection)
    2. Captures stdout and stderr independently
    3. Waits for process completion (blocking)
    4. Decodes bytes to UTF-8 strings
    5. Optionally prints command, stdout, stderr if not silent

    Used extensively for GDAL command-line tools:
    - gdal_translate: Format conversion, COG generation
    - gdalwarp: Reprojection, resampling
    - gdalbuildvrt: Virtual raster mosaics
    - gdalinfo: Metadata inspection

    Args:
        command: Shell command string (may include pipes, redirects)
        silent: If False, print command and output (default: True for logs)

    Returns:
        tuple: (stdout_str, stderr_str)
    """
    if not silent:
        print(command)
    p = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = p.communicate()
    err = stderr.decode()
    if err != "" and not silent:
        print(err)
    out = stdout.decode()
    if out != "" and not silent:
        print(out)
    return out, err


def create_folder(path):
    """Create directory path if missing (mkdir -p)."""
    folder_path = Path(path)
    folder_path.mkdir(parents=True, exist_ok=True)


def get_aggregation_ids():
    """Return aggregation ids ordered oldest to newest."""
    return list(sorted([path.split("/")[-1] for path in glob(f"aggregation-store/*")]))


def get_vertical_rounding_multiplier(z):
    """Return rounding factor used for vertical precision at zoom z."""
    return int(2 ** ((10 - z) / 2) / (1 / 256))


def save_terrarium_tile(data, filepath):
    """Encode elevation data as Terrarium WebP tile at given filepath.

    Encoding algorithm:
    1. Precision rounding (zoom-dependent):
       - full_resolution_zoom = 19: Use full 1/256m Terrarium precision
       - Lower zooms: factor = 2^((19-z)/2) / 256
       - Round elevation to factor multiples (e.g., z=12 → ~0.5m steps)
       - Prevents false precision at coarse pixel resolutions

    2. Terrarium RGB mapping:
       - Offset: elevation_adjusted = elevation + 32768
       - Red channel: floor(elevation_adjusted / 256)
       - Green channel: elevation_adjusted % 256
       - Blue channel: fractional part × 256
       - Decoding: height = R×256 + G + B/256 - 32768

    3. WebP compression:
       - Lossless mode: Preserves exact RGB values
       - Typical compression: 60-70% of raw 512×512×3 PNG
       - Widely supported format for web delivery

    The zoom-based rounding ensures tiles don't encode spurious precision:
    at z=12 with 38m/pixel resolution, sub-meter elevation precision is
    meaningless and wastes bandwidth.

    Args:
        data: 512×512 float32 elevation array (meters)
        filepath: Output .webp path (e.g., tmp/{z}-{x}-{y}.webp)
    """
    filename = filepath.split("/")[-1]
    z = int(filename.split("-")[0])

    # full terrarium resolution of 1/256 at `full_resolution_zoom`
    # multiples of 2 of full terrarium resolution at lower zooms
    full_resolution_zoom = 19
    factor = 2 ** (full_resolution_zoom - z) / 256
    data = np.round(data / factor) * factor

    data += 32768
    rgb = np.zeros((512, 512, 3), dtype=np.uint8)
    rgb[..., 0] = data // 256
    rgb[..., 1] = data % 256
    rgb[..., 2] = (data - np.floor(data)) * 256
    with open(filepath, "wb") as f:
        f.write(imagecodecs.webp_encode(rgb, lossless=True))


def create_archive(tmp_folder, out_filepath):
    """Package WebP tiles from a tmp folder into a PMTiles archive.

    Assembly process:
    1. Scan tmp_folder for *.webp files
    2. Parse z-x-y coordinates from filenames
    3. Convert to tile_ids: tile_id = (z × 2^z + x) × 2^z + y
    4. Sort tile_ids (ensures sequential writes for optimal compression)
    5. For each tile in sorted order:
       a. Read WebP bytes from file
       b. writer.write_tile: Append to PMTiles with internal indexing
       c. Track bbox: min/max lon/lat from tile bounds
       d. Track zoom range: min/max z values
    6. writer.finalize: Write directory (tile index) and header
       - Header includes: bbox, zoom range, tile type (PNG/WebP/etc.)
       - Directory enables O(log n) tile lookups by tile_id
       - Compression: Tiles stored with their original WebP compression

    The resulting PMTiles file supports HTTP range requests, enabling
    efficient on-demand tile serving from static storage without a server.

    Args:
        tmp_folder: Directory containing z-x-y.webp tiles
        out_filepath: Output PMTiles path (e.g., pmtiles-store/12-2130-1459-17.pmtiles)
    """
    with open(out_filepath, "wb") as f1:
        writer = Writer(f1)
        min_z = math.inf
        max_z = 0
        min_lon = math.inf
        min_lat = math.inf
        max_lon = -math.inf
        max_lat = -math.inf

        tile_ids = []
        for filepath in glob(f"{tmp_folder}/*.webp"):
            filename = filepath.split("/")[-1]
            z, x, y = [int(a) for a in filename.replace(".webp", "").split("-")]
            tile_ids.append(zxy_to_tileid(z=z, x=x, y=y))
        tile_ids = sorted(tile_ids)

        for tile_id in tile_ids:
            z, x, y = tileid_to_zxy(tile_id)
            filepath = f"{tmp_folder}/{z}-{x}-{y}.webp"
            with open(filepath, "rb") as f2:
                writer.write_tile(tile_id, f2.read())

            max_z = max(max_z, z)
            min_z = min(min_z, z)
            west, south, east, north = mercantile.bounds(x, y, z)
            min_lon = min(min_lon, west)
            min_lat = min(min_lat, south)
            max_lon = max(max_lon, east)
            max_lat = max(max_lat, north)

        min_lon_e7 = int(min_lon * 1e7)
        min_lat_e7 = int(min_lat * 1e7)
        max_lon_e7 = int(max_lon * 1e7)
        max_lat_e7 = int(max_lat * 1e7)

        writer.finalize(
            {
                "tile_type": TileType.WEBP,
                "tile_compression": Compression.NONE,
                "min_zoom": min_z,
                "max_zoom": max_z,
                "min_lon_e7": min_lon_e7,
                "min_lat_e7": min_lat_e7,
                "max_lon_e7": max_lon_e7,
                "max_lat_e7": max_lat_e7,
                "center_zoom": int(0.5 * (min_z + max_z)),
                "center_lon_e7": int(0.5 * (min_lon_e7 + max_lon_e7)),
                "center_lat_e7": int(0.5 * (min_lat_e7 + max_lat_e7)),
            },
            {
                "attribution": '<a href="https://mapterhorn.com/attribution">© Mapterhorn</a>'
            },
        )


def get_aggregation_item_string(aggregation_id, filename):
    """Read aggregation CSV content into a normalized string."""
    result = ""
    filepath = f"aggregation-store/{aggregation_id}/{filename}"
    if not os.path.isfile(filepath):
        return None

    with open(filepath) as f:
        result = "".join([l.strip() for l in f.readlines()])

    return result.strip()


def get_dirty_aggregation_filenames(current_aggregation_id, last_aggregation_id):
    """Compare two aggregation runs and return filenames that changed."""
    filepaths = sorted(
        glob(f"aggregation-store/{current_aggregation_id}/*-aggregation.csv")
    )

    if last_aggregation_id is None:
        return [filepath.split("/")[-1] for filepath in filepaths]

    dirty_filenames = []
    for filepath in filepaths:
        filename = filepath.split("/")[-1]
        current = get_aggregation_item_string(current_aggregation_id, filename)
        last = get_aggregation_item_string(last_aggregation_id, filename)
        if current != last:
            dirty_filenames.append(filename)
    return dirty_filenames


def get_pmtiles_folder(x, y, z):
    """Return folder path for storing a pmtiles based on tile coordinates."""
    if z < 7:
        return f"pmtiles-store"
    if z == 7:
        return f"pmtiles-store/{z}-{x}-{y}"
    else:
        parent = mercantile.parent(mercantile.Tile(x=x, y=y, z=z), zoom=7)
        return f"pmtiles-store/{parent.z}-{parent.x}-{parent.y}"


# group source items by maxzoom and source
def get_grouped_source_items(filepath):
    """Group source rows by maxzoom and source to preserve priority order.

    Priority sorting algorithm:
    1. Parse aggregation CSV rows (source, filename, maxzoom)
    2. Sort by (-maxzoom, source):
       - Negative maxzoom ensures DESC order (higher res first)
       - Source name provides ASC tiebreaker (alphabetical)
    3. Group consecutive rows with same (maxzoom, source) signature
    4. Return list of groups in priority order

    Example priority order:
    - Group 0: usgs-1-3-arc-second (z=17) - highest priority
    - Group 1: usgs-1-arc-second (z=15)
    - Group 2: glo-30 (z=12) - fills remaining gaps

    This ordering ensures aggregation_reproject and aggregation_merge
    process highest-quality sources first, using lower-quality data
    only to patch nodata gaps.

    Args:
        filepath: Path to aggregation CSV

    Returns:
        list: Grouped source items [[{source, filename, maxzoom}]]
    """
    lines = []
    with open(filepath) as f:
        lines = f.readlines()
    lines = lines[1:]  # skip header
    line_tuples = []
    for line in lines:
        source, filename, maxzoom = line.strip().split(",")
        maxzoom = int(maxzoom)
        line_tuples.append((-maxzoom, source, filename))
    line_tuples = sorted(line_tuples)
    grouped_source_items = []

    first_line_tuple = line_tuples[0]
    last_group_signature = (first_line_tuple[0], first_line_tuple[1])
    current_group = [
        {
            "maxzoom": -first_line_tuple[0],
            "source": first_line_tuple[1],
            "filename": first_line_tuple[2],
        }
    ]
    for line_tuple in line_tuples[1:]:
        current_group_signature = (line_tuple[0], line_tuple[1])
        if current_group_signature != last_group_signature:
            grouped_source_items.append(current_group)
            current_group = []
            last_group_signature = current_group_signature
        current_group.append(
            {
                "maxzoom": -line_tuple[0],
                "source": line_tuple[1],
                "filename": line_tuple[2],
            }
        )
    grouped_source_items.append(current_group)
    return grouped_source_items


class HashWriter:
    """Wrapper that proxies writes while tracking an md5 digest."""

    def __init__(self, f):
        self.f = f
        self.md5 = hashlib.md5()

    def write(self, data):
        self.md5.update(data)
        return self.f.write(data)

    def tell(self):
        return self.f.tell()

    def flush(self):
        return self.f.flush()

    def close(self):
        return self.f.close()
