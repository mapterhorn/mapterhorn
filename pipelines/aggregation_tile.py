"""Slice merged GeoTIFFs into Terrarium WebP tiles and package as PMTiles.

This module converts seamlessly-blended aggregation rasters into the final
tile format used for client delivery:

**Input**: Merged COG from aggregation_merge.py with 512×512 internal tiling,
nodata filled, seams blended, and buffer zones still attached.

**Tiling process**:
1. Verify merge-done marker exists (dependency)
2. Read buffer_pixels from reprojection.json metadata
3. Iterate through COG's 512×512 internal blocks:
   a. Skip buffer zones at edges using buffer_pixels offset
   b. Read elevation data for each child tile
   c. Replace nodata (-9999) with 0 (sea level default)
   d. Encode as Terrarium RGB: R=elev//256, G=elev%256, B=frac*256
      where elevation = R*256 + G + B/256 - 32768 (meters)
   e. Compress as lossless WebP (preserves exact values)
4. Package all WebP tiles into PMTiles archive
5. Write pmtiles-done marker

**Terrarium encoding**: Maps -32768m to 32767.99m into RGB [0,255]
with 1/256m (~4mm) precision, suitable for Earth's elevation range.

**PMTiles format**: Single-file archive with internal tile index,
enabling range-request access from static storage (R2, S3, etc.).

**Output**: pmtiles-store/{parent}/{z-x-y-childz}.pmtiles ready for
bundling into regional/planet archives.
"""

from glob import glob
import math
import os
import json

import mercantile
import rasterio

import utils


def create_tiles(tmp_folder, aggregation_tile, tiff_filepath, buffer_pixels):
    """Slice merged raster into child tiles and write Terrarium WebP outputs.

    Grid computation:
    1. Verify COG has 512×512 internal tiling (required for efficient access)
    2. Calculate horizontal_block_count = (width - 2*buffer) / 512
    3. Derive child_z from aggregation tile:
       child_z = base_z + log2(horizontal_block_count)
    4. Compute child tile range:
       x_min = base_x * 2^(child_z - base_z)
       y_min = base_y * 2^(child_z - base_z)
       tile_count = 2^(child_z - base_z) in each dimension
    5. For each child tile in grid:
       create_tile(i, j, ...) extracts 512x512 block + buffer offset

    The child_z determines the final zoom level of output tiles, typically
    z=13-17 depending on source resolution. Buffer trimming ensures tiles
    are exact 512×512 without overlap, ready for PMTiles packaging.

    Args:
        tmp_folder: Working directory for WebP output
        aggregation_tile: Parent tile coordinates
        tiff_filepath: Merged COG with buffers
        buffer_pixels: Pixels to trim from edges (from reprojection.json)
    """
    base_x = aggregation_tile.x
    base_y = aggregation_tile.y
    base_z = aggregation_tile.z

    child_z = None
    with rasterio.open(tiff_filepath) as src:
        assert len(src.block_shapes) >= 1
        assert src.block_shapes[0] == (512, 512)
        horizontal_block_count = (src.width - 2 * buffer_pixels) / 512
        assert math.floor(horizontal_block_count) == horizontal_block_count
        child_z = base_z + int(math.log2(horizontal_block_count))
    z = child_z
    x_min = base_x * 2 ** (z - base_z)
    y_min = base_y * 2 ** (z - base_z)
    for i, x in enumerate(range(x_min, x_min + 2 ** (z - base_z))):
        for j, y in enumerate(range(y_min, y_min + 2 ** (z - base_z))):
            out_filepath = f"{tmp_folder}/{z}-{x}-{y}.webp"
            create_tile(i, j, tiff_filepath, out_filepath, buffer_pixels)


def create_tile(i, j, tiff_filepath, out_filepath, buffer_pixels):
    """Extract a single 512x512 tile with buffer trimming and save as WebP."""
    col_start = i * 512 + buffer_pixels
    col_end = (i + 1) * 512 + buffer_pixels
    row_start = j * 512 + buffer_pixels
    row_end = (j + 1) * 512 + buffer_pixels
    window = rasterio.windows.Window(
        col_off=col_start,
        row_off=row_start,
        width=col_end - col_start,
        height=row_end - row_start,
    )
    subdata = None
    with rasterio.open(tiff_filepath) as src:
        subdata = src.read(1, window=window, out_shape=(512, 512))
    subdata[subdata == -9999] = 0
    utils.save_terrarium_tile(subdata, out_filepath)


def main(filepath):
    """Create PMTiles archive for an aggregation tile if merge is completed."""
    _, aggregation_id, filename = filepath.split("/")

    z, x, y, child_z = [
        int(a) for a in filename.replace("-aggregation.csv", "").split("-")
    ]

    tmp_folder = f"aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-tmp"

    pmtiles_done_filepath = f"{tmp_folder}/pmtiles-done"
    if os.path.isfile(pmtiles_done_filepath):
        print(f"tiling {filename} already done...")
        return

    merge_done = os.path.isfile(f"{tmp_folder}/merge-done")
    if not merge_done:
        print("merge not done yet...")
        return

    buffer_pixels = None
    with open(f"{tmp_folder}/reprojection.json") as f:
        metadata = json.load(f)
        buffer_pixels = metadata["buffer_pixels"]

    num_tiff_files = len(glob(f"{tmp_folder}/*.tiff"))
    tiff_filepath = f"{tmp_folder}/{num_tiff_files - 1}-3857.tiff"

    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)
    out_folder = utils.get_pmtiles_folder(x, y, z)
    utils.create_folder(out_folder)
    out_filepath = f"{out_folder}/{z}-{x}-{y}-{child_z}.pmtiles"
    create_tiles(tmp_folder, aggregation_tile, tiff_filepath, buffer_pixels)
    utils.create_archive(tmp_folder, out_filepath)
    utils.run_command(f"touch {pmtiles_done_filepath}")
