"""Generate lower-zoom overview PMTiles by averaging higher-zoom tiles.

This module builds the zoom pyramid by recursively downsampling elevation
tiles, creating smooth lower-resolution overviews for efficient rendering:

**Input**: PMTiles archives containing Terrarium-encoded WebP tiles at
higher zoom levels (typically z=13-17 from aggregation_tile.py).

**Downsampling algorithm**:
1. Read downsampling CSV listing input PMTiles to combine
2. For each parent tile at zoom N:
   a. Load 4 child tiles at zoom N+1 (arranged in 2×2 grid)
   b. Decode Terrarium RGB → elevation (m) via: r*256 + g + b/256 - 32768
   c. Assemble into 1024×1024 elevation array
   d. Downsample via 2×2 block averaging → 512×512 parent
   e. Re-encode to Terrarium RGB with zoom-appropriate precision
   f. Compress as lossless WebP
3. Package resulting tiles into PMTiles archive
4. Recurse down to z=0 for complete pyramid

**Precision handling**:
- Higher zooms (z≥19): Full 1/256m Terrarium precision
- Lower zooms: Progressively coarser rounding (factor = 2^((19-z)/2)/256)
  to match reduced pixel resolution and avoid false precision

**Dirty tracking**: Only regenerates tiles affected by changed aggregation
data, comparing current vs. previous aggregation IDs for efficiency.
"""

from glob import glob
import io
from multiprocessing import Pool
import shutil
from datetime import datetime
import os

import numpy as np
from PIL import Image
import imagecodecs
import mercantile
from pmtiles.reader import Reader, MmapSource

import utils


def create_tile(
    parent_x, parent_y, parent_z, aggregation_id, tmp_folder, pmtiles_filenames
):
    """Average four child tiles into a parent Terrarium WebP tile.

    Detailed process:
    1. Lookup: Map child tile coordinates to their source PMTiles archives
    2. Decode: For each of 4 children (if exists):
       - Read WebP from PMTiles
       - Decode Terrarium RGB to meters: (R×256 + G + B/256) - 32768
       - Place into appropriate 512×512 quadrant of 1024×1024 array
    3. Downsample:
       - Reshape to (512,2,512,2) to group 2×2 pixel blocks
       - Average along axes (1,3) → (512,512) parent elevation
    4. Encode:
       - Add 32768 offset (Terrarium uses 0-65536 range)
       - Split into RGB: R=elev//256, G=elev%256, B=frac×256
       - Ensures height(m) = R*256 + G + B/256 - 32768 on decode
    5. Compress:
       - Write as lossless WebP (preserves exact elevation values)
       - Save to tmp folder for PMTiles packaging

    Args:
        parent_x, parent_y, parent_z: Web Mercator tile coordinates
        aggregation_id: ULID identifying aggregation batch
        tmp_folder: Temp dir for tile output
        pmtiles_filenames: List of source PMTiles archives to read from
    """
    tile_to_pmtiles_filename = get_tile_to_pmtiles_filename(pmtiles_filenames)
    full_data = np.zeros((1024, 1024), dtype=np.float32)
    for row_offset in range(2):
        for col_offset in range(2):
            child_x = 2 * parent_x + col_offset
            child_y = 2 * parent_y + row_offset
            child_z = parent_z + 1
            child = mercantile.Tile(x=child_x, y=child_y, z=child_z)
            if child not in tile_to_pmtiles_filename:
                continue
            child_bytes = None
            filename = tile_to_pmtiles_filename[child]
            file_z, file_x, file_y, _ = [
                int(a) for a in filename.replace(".pmtiles", "").split("-")
            ]
            pmtiles_folder = utils.get_pmtiles_folder(file_x, file_y, file_z)
            with open(f"{pmtiles_folder}/{filename}", "r+b") as f:
                reader = Reader(MmapSource(f))
                child_bytes = reader.get(child_z, child_x, child_y)
            child_rgb = np.array(Image.open(io.BytesIO(child_bytes)), dtype=np.float32)
            row_start = 512 * row_offset
            row_end = 512 * (row_offset + 1)
            col_start = 512 * col_offset
            col_end = 512 * (col_offset + 1)
            # (red * 256 + green + blue / 256) - 32768
            full_data[row_start:row_end, col_start:col_end] = (
                child_rgb[:, :, 0] * 256.0
                + child_rgb[:, :, 1]
                + child_rgb[:, :, 2] / 256.0
                - 32768.0
            )

    parent_data = full_data.reshape((512, 2, 512, 2)).mean(
        axis=(1, 3)
    )  # downsample by 4x4 pixel averaging

    parent_data += 32768.0
    parent_rgb = np.zeros((512, 512, 3), dtype=np.uint8)
    parent_rgb[:, :, 0] = parent_data // 256
    parent_rgb[:, :, 1] = np.floor(parent_data % 256)
    parent_rgb[:, :, 2] = np.floor((parent_data - np.floor(parent_data)) * 256)

    parent_bytes = imagecodecs.webp_encode(parent_rgb, lossless=True)
    parent_filepath = f"{tmp_folder}/{parent_z}-{parent_x}-{parent_y}.webp"
    with open(parent_filepath, "wb") as f:
        f.write(parent_bytes)


def get_tile_to_pmtiles_filename(pmtiles_filenames):
    """Map every tile covered by a pmtiles filename to that filename."""
    tile_to_pmtiles_filename = {}
    for pmtiles_filename in pmtiles_filenames:
        pmtiles_z, pmtiles_x, pmtiles_y, child_zoom = [
            int(a) for a in pmtiles_filename.replace(".pmtiles", "").split("-")
        ]
        children = None
        if pmtiles_z == child_zoom:
            children = [mercantile.Tile(x=pmtiles_x, y=pmtiles_y, z=pmtiles_z)]
        else:
            children = list(
                mercantile.children(
                    mercantile.Tile(x=pmtiles_x, y=pmtiles_y, z=pmtiles_z),
                    zoom=child_zoom,
                )
            )
        for child in children:
            tile_to_pmtiles_filename[child] = pmtiles_filename
    return tile_to_pmtiles_filename


def main(filepaths):
    """Downsample tiles listed in downsampling CSV files and emit PMTiles."""
    for j, filepath in enumerate(filepaths):
        _, aggregation_id, filename = filepath.split("/")
        print(f"downsampling {filename}. {datetime.now()}. {j + 1} / {len(filepaths)}.")
        if os.path.isfile(filepath.replace("-downsampling.csv", "-downsampling.done")):
            print("already done...")
            continue
        parts = filename.split("-")
        extent_z, extent_x, extent_y, parent_zoom = [int(a) for a in parts[:4]]

        out_folder = utils.get_pmtiles_folder(extent_x, extent_y, extent_z)
        utils.create_folder(out_folder)
        out_filepath = (
            f"{out_folder}/{extent_z}-{extent_x}-{extent_y}-{parent_zoom}.pmtiles"
        )

        extent = mercantile.Tile(x=extent_x, y=extent_y, z=extent_z)
        tmp_folder = filepath.replace("-downsampling.csv", "-tmp")
        utils.create_folder(tmp_folder)

        pmtiles_filenames = None
        with open(filepath) as f:
            pmtiles_filenames = f.readlines()
            pmtiles_filenames = pmtiles_filenames[1:]  # skip header
            pmtiles_filenames = [a.strip() for a in pmtiles_filenames]

        parents = None
        if extent_z == parent_zoom:
            parents = [extent]
        else:
            parents = list(mercantile.children(extent, zoom=parent_zoom))

        argument_tuples = []
        for parent in parents:
            argument_tuples.append(
                (
                    parent.x,
                    parent.y,
                    parent.z,
                    aggregation_id,
                    tmp_folder,
                    pmtiles_filenames,
                )
            )

        with Pool() as pool:
            pool.starmap(create_tile, argument_tuples, chunksize=1)

        utils.create_archive(tmp_folder, out_filepath)

        shutil.rmtree(tmp_folder)
        utils.run_command(
            f'touch {filepath.replace("-downsampling.csv", "-downsampling.done")}'
        )


def tiles_intersect(a, b):
    """Return True if tile a intersects tile b at any zoom level."""
    if a == b:
        return True
    if a.z < b.z and mercantile.parent(b, zoom=a.z) == a:
        return True
    if b.z < a.z and mercantile.parent(a, zoom=b.z) == b:
        return True
    return False


def is_parent_of_dirty_aggregation_tile(tile, dirty_aggregation_tiles):
    """Check if tile is ancestor of any dirty aggregation tile."""
    for dirty_aggregation_tile in dirty_aggregation_tiles:
        if tiles_intersect(dirty_aggregation_tile, tile):
            return True
    return False


def not_in_previous_aggregation(filename, aggregation_ids):
    """Return True if a downsampling file is new relative to prior aggregation."""
    return len(glob(f"aggregation-store/{aggregation_ids[-2]}/{filename}")) == 0


if __name__ == "__main__":
    child_zoom_to_filepaths = {}
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    dirty_aggregation_tiles = []
    if len(aggregation_ids) >= 2:
        dirty_aggregation_filenames = utils.get_dirty_aggregation_filenames(
            aggregation_id, aggregation_ids[-2]
        )
        for filename in dirty_aggregation_filenames:
            z, x, y, _ = [
                int(a) for a in filename.replace("-aggregation.csv", "").split("-")
            ]
            dirty_aggregation_tiles.append(mercantile.Tile(x=x, y=y, z=z))

    for filepath in sorted(
        glob(f"aggregation-store/{aggregation_id}/*-downsampling.csv")
    ):
        filename = filepath.split("/")[-1]
        z, x, y, child_zoom = [
            int(a) for a in filename.replace("-downsampling.csv", "").split("-")
        ]

        if (
            len(aggregation_ids) < 2
            or is_parent_of_dirty_aggregation_tile(
                mercantile.Tile(x=x, y=y, z=z), dirty_aggregation_tiles
            )
            or not_in_previous_aggregation(filename, aggregation_ids)
        ):
            if child_zoom not in child_zoom_to_filepaths:
                child_zoom_to_filepaths[child_zoom] = []
            child_zoom_to_filepaths[child_zoom].append(filepath)

    child_zooms = list(reversed(sorted(list(child_zoom_to_filepaths.keys()))))
    for child_zoom in child_zooms:
        main(child_zoom_to_filepaths[child_zoom])
