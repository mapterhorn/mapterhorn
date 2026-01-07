"""Combine individual PMTiles into regional/planet bundles with dirty tracking.

This module creates the final distribution archives by intelligently bundling
smaller PMTiles files while minimizing unnecessary rebuilds:

**Bundling hierarchy**:
1. Planet bundle (z=0): All tiles at z≤12 (low-res global coverage)
2. Regional bundles (z=6): Tiles at z=13+ grouped by parent z=6 tile
   (64°64 degree regions at equator, ~7100km square)

**Dirty tile detection**:
- Compares current aggregation ID against previous run's aggregations
- Identifies which aggregation tiles changed or were added
- Derives parent z=6 tiles affected by changed aggregations
- Only rebuilds bundles containing dirty parents + planet bundle
- Dramatically reduces rebuild time (minutes vs. hours for full rebuild)

**Bundling algorithm**:
1. get_parent_to_filepaths: Group child PMTiles by parent z=6 tile
2. For each dirty parent:
   a. Read all child PMTiles into memory (tile_id → bytes maps)
   b. Merge tile dictionaries, with higher-priority sources overwriting
   c. Sort by tile_id for efficient sequential access
   d. Write combined PMTiles with updated bbox/zoom metadata
   e. Compute SHA256 checksum for verify_upload.py
3. Write parent PMTiles to bundle-store/

**Output**: Regional bundles (e.g., 6-32-21.pmtiles for North America)
and planet.pmtiles, ready for CDN distribution via upload.py.
"""

from glob import glob
import math
import time

import mercantile
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.reader import Reader, MmapSource, all_tiles
from pmtiles.writer import Writer

import utils


def get_parent_to_filepaths(only_dirty=True):
    """Group child PMTiles filepaths by parent tile, optionally only dirty ones."""
    filepaths = sorted(
        glob("pmtiles-store/*.pmtiles") + glob("pmtiles-store/*/*.pmtiles")
    )

    parent_to_filepath = {}
    dirty_parents = get_dirty_parents()

    for filepath in filepaths:
        filename = filepath.split("/")[-1]
        z, x, y, child_z = [int(a) for a in filename.replace(".pmtiles", "").split("-")]

        parent = None
        if child_z <= 12:
            parent = mercantile.Tile(x=0, y=0, z=0)
        else:
            assert z >= 6
            if z == 6:
                parent = mercantile.Tile(x=x, y=y, z=z)
            else:
                parent = mercantile.parent(mercantile.Tile(x=x, y=y, z=z), zoom=6)

        if only_dirty and parent not in dirty_parents:
            continue

        if parent not in parent_to_filepath:
            parent_to_filepath[parent] = []

        parent_to_filepath[parent].append(filepath)

    return parent_to_filepath


def get_dirty_parents():
    """Return parent tiles that contain changes since the last aggregation.

    Change detection algorithm:
    1. Always mark planet (z=0) bundle as dirty (contains all z≤12 tiles)
    2. Load current and previous aggregation IDs from aggregation-store/
    3. Call get_dirty_aggregation_filenames to find changed CSVs:
       - New aggregation items (only in current)
       - Modified items (different source composition)
    4. For each dirty aggregation tile:
       - If child_z ≥ 13 (high-zoom regional data):
         * Extract aggregation tile coordinates (z, x, y)
         * Compute parent z=6 tile via mercantile.parent
         * Add to dirty set
    5. Return unique set of parent tiles needing rebuild

    This enables incremental bundling: when only a few source regions change
    (e.g., updated USGS 1/3 arc-second for one state), only the affected
    regional bundles rebuild instead of reprocessing the entire planet.

    Returns:
        list: Parent Tile objects (z=0 or z=6) requiring bundle regeneration
    """
    dirty_parents = set([mercantile.Tile(x=0, y=0, z=0)])
    aggregation_ids = utils.get_aggregation_ids()
    assert len(aggregation_ids) > 0
    current_aggregation_id = aggregation_ids[-1]
    last_aggregation_id = None if len(aggregation_ids) == 1 else aggregation_ids[-2]
    aggregation_filenames = utils.get_dirty_aggregation_filenames(
        current_aggregation_id, last_aggregation_id
    )

    for filename in aggregation_filenames:
        z, x, y, child_z = [
            int(a) for a in filename.replace("-aggregation.csv", "").split("-")
        ]
        if child_z >= 13:
            dirty_parents.add(mercantile.parent(mercantile.Tile(x=x, y=y, z=z), zoom=6))

    return list(dirty_parents)


def read_full_archive(filepath):
    """Read an entire PMTiles file into a tile_id -> bytes map."""
    tile_id_to_bytes = {}
    with open(filepath, "r+b") as f2:
        reader = Reader(MmapSource(f2))
        for tile_tuple, tile_bytes in all_tiles(reader.get_bytes):
            tile_id = zxy_to_tileid(*tile_tuple)
            tile_id_to_bytes[tile_id] = tile_bytes
    return tile_id_to_bytes


def create_archive(filepaths, out_filepath):
    """Create a bundled PMTiles archive from provided child archive paths.

    Archive assembly:
    1. Open output file with HashWriter for SHA256 computation
    2. Initialize PMTiles Writer with metadata tracking:
       - min_z, max_z: Zoom range covered by bundle
       - bbox: Geographic extent (min_lon, min_lat, max_lon, max_lat)
    3. Build tile index:
       a. For each child PMTiles file:
          * Parse z-x-y-childz from filename
          * If z == childz: Single tile (macrotile aggregation)
          * Else: Expand to child tiles via mercantile.children
          * Convert to tile_id for PMTiles indexing
       b. Sort (tile_id, filepath) pairs for efficient streaming
    4. Read and write tiles:
       a. Group consecutive tiles from same source file
       b. read_full_archive: Load entire source PMTiles to memory
       c. Extract tile bytes by tile_id from in-memory map
       d. writer.write_tile: Append to output with automatic compression
       e. Update bbox and zoom metadata from tile coordinates
    5. writer.finalize: Write directory and header to complete archive
    6. Return SHA256 checksum (hex string) for manifest

    The tile_id sorting ensures sequential disk writes, and grouping reads
    from the same source minimizes file open/close overhead.

    Args:
        filepaths: List of child PMTiles paths to bundle
        out_filepath: Output bundle path (e.g., bundle-store/6-32-21.pmtiles)

    Returns:
        str: SHA256 checksum of complete archive
    """
    checksum = None
    with open(out_filepath, "wb") as f1:
        hash_writer = utils.HashWriter(f1)
        writer = Writer(hash_writer)
        min_z = math.inf
        max_z = 0
        min_lon = math.inf
        min_lat = math.inf
        max_lon = -math.inf
        max_lat = -math.inf

        tile_ids_and_filepaths = []

        j = 0
        for filepath in filepaths:
            filename = filepath.split("/")[-1]
            z, x, y, child_z = [
                int(a) for a in filename.replace(".pmtiles", "").split("-")
            ]
            parent = mercantile.Tile(x=x, y=y, z=z)
            tiles = []
            if z == child_z:
                tiles.append(parent)
            else:
                tiles += mercantile.children(parent, zoom=child_z)
            for tile in tiles:
                tile_id = zxy_to_tileid(tile.z, tile.x, tile.y)
                tile_ids_and_filepaths.append((tile_id, filepath))

            max_z = max(max_z, child_z)
            min_z = min(min_z, child_z)
            west, south, east, north = mercantile.bounds(x, y, z)
            min_lon = min(min_lon, west)
            min_lat = min(min_lat, south)
            max_lon = max(max_lon, east)
            max_lat = max(max_lat, north)
            j += 1
            if j % 1000 == 0:
                print(f"prepared {j:_} / {len(filepaths):_} filepaths...")

        tile_ids_and_filepaths = sorted(tile_ids_and_filepaths)

        last_filepath = None
        tile_id_to_bytes = None

        j = 0
        start = time.time()
        for tile_id, filepath in tile_ids_and_filepaths:
            if filepath != last_filepath:
                last_filepath = filepath
                tile_id_to_bytes = read_full_archive(filepath)
            writer.write_tile(tile_id, tile_id_to_bytes[tile_id])

            j += 1
            if j % 10_000 == 0:
                tic = time.time()
                time_so_far = tic - start
                expected_duration = time_so_far * len(tile_ids_and_filepaths) / j
                finishes_in = expected_duration - time_so_far
                print(
                    f"Processed {j:_} / {len(tile_ids_and_filepaths):_} tiles in {int(time_so_far / 60)} min {int(time_so_far) % 60} s. Finishes in {int(finishes_in / 3600)} h {int(finishes_in / 60) % 60} min..."
                )

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
                "attribution": '<a href="https://mapterhorn.com/attribution">© Mapterhorn</a>',
            },
        )
        checksum = hash_writer.md5.hexdigest()

    with open(f"{out_filepath}.md5", "w") as f:
        f.write(f"{checksum} {out_filepath.split('/')[-1]}\n")


def get_name_from_parent(parent):
    """Derive output name (planet or z-x-y) for a parent tile."""
    name = None
    if parent == mercantile.Tile(x=0, y=0, z=0):
        name = "planet"
    else:
        name = f"{parent.z}-{parent.x}-{parent.y}"
    return name


def main():
    """Bundle dirty PMTiles into parent-level archives in bundle-store."""
    parent_to_filepaths = get_parent_to_filepaths()
    for parent in parent_to_filepaths:
        name = get_name_from_parent(parent)
        print(name)
        folder = f"bundle-store/{name}"
        utils.create_folder(folder)
        out_filepath = f"{folder}/{name}.pmtiles"
        create_archive(parent_to_filepaths[parent], out_filepath)


if __name__ == "__main__":
    main()
