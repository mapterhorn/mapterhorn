"""Reproject source rasters into buffered EPSG:3857 tiles for aggregation merging.

This module transforms source DEM data from arbitrary CRS (typically EPSG:4326
or local projections) into standardized Web Mercator tiles with:

**Reprojection workflow**:
1. Group source files by priority (maxzoom DESC, source ASC)
2. For each priority group:
   a. Build GDAL VRT combining all files in group
   b. Warp VRT to EPSG:3857 with cubicspline resampling
   c. Set target resolution matching zoom level's pixel size
   d. Add buffer zone (150m per macrotile = 300m total) around tile bounds
   e. Translate to COG with 512×512 tiling, sparse optimization
3. Verify output contains nodata pixels (if not, no gaps to fill)
4. Write reprojection metadata for merge stage

**Buffering strategy**:
The buffer zones enable seamless blending across tile boundaries:
- Macrotiles at z=12 add 2×150m = 300m buffer
- Larger aggregation tiles multiply buffer by their zoom difference
- Ensures ~50-100px overlap at target resolution for Gaussian blending
- Buffers are cropped after merging to prevent duplicate coverage

**Resampling**: Cubicspline interpolation preserves smooth terrain while
avoiding the ringing artifacts of Lanczos and blockiness of bilinear.

**Output**: Priority-numbered COGs (0-3857.tiff = highest priority) ready
for gap-filling merge in aggregation_merge.py.
"""

import json
import os

import rasterio
import mercantile

import utils

SILENT = True


def create_virtual_raster(tmp_folder, i, source_items):
    """Build a VRT from grouped source files for a single aggregation tile."""
    source = source_items[0]["source"]
    vrt_filepath = f"{tmp_folder}/{i}.vrt"
    input_file_list_path = f"{tmp_folder}/{i}-file-list.txt"
    with open(input_file_list_path, "w") as f:
        for source_item in source_items:
            f.write(f'source-store/{source}/{source_item["filename"]}\n')
    command = f"gdalbuildvrt -overwrite -input_file_list {input_file_list_path} {vrt_filepath}"
    out, err = utils.run_command(command, silent=SILENT)
    if not SILENT:
        print(out, err)
    return vrt_filepath


def get_resolution(zoom):
    """Return pixel resolution (meters/pixel) for a 512px tile at zoom."""
    tile = mercantile.Tile(x=0, y=0, z=zoom)
    bounds = mercantile.xy_bounds(tile)
    return (bounds.right - bounds.left) / 512


def create_warp(vrt_filepath, vrt_3857_filepath, zoom, aggregation_tile, buffer):
    """Warp a VRT into EPSG:3857 with buffer and target resolution/extent."""
    left, bottom, right, top = mercantile.xy_bounds(aggregation_tile)
    left -= buffer
    bottom -= buffer
    right += buffer
    top += buffer
    resolution = get_resolution(zoom)
    command = f"gdalwarp -of vrt -overwrite "
    command += f"-t_srs EPSG:3857 "
    command += f"-tr {resolution} {resolution} "
    command += f"-te {left} {bottom} {right} {top} "
    command += f"-r cubicspline "
    command += f"-dstnodata -9999 "
    command += f"{vrt_filepath} {vrt_3857_filepath}"
    out, err = utils.run_command(command, silent=SILENT)
    if err.strip() != "":
        raise Exception(f"gdalwarp failed for {vrt_filepath}:\n{out}\n{err}")


def translate(in_filepath, out_filepath):
    """Convert a VRT to a COG with tiling and no compression."""
    command = f"GDAL_CACHEMAX=512 gdal_translate -of COG "
    command += f"-co BIGTIFF=IF_NEEDED -co ADD_ALPHA=YES -co OVERVIEWS=NONE "
    command += f"-co SPARSE_OK=YES -co BLOCKSIZE=512 -co COMPRESS=NONE "
    command += f"{in_filepath} "
    command += f"{out_filepath}"
    out, err = utils.run_command(command, silent=SILENT)
    if err.strip() != "":
        raise Exception(f"gdal_translate failed for {in_filepath}:\n{out}\n{err}")


def contains_nodata_pixels(filepath):
    """Return True if the raster contains any -9999 nodata values."""
    with rasterio.env.Env(GDAL_CACHEMAX=64):
        with rasterio.open(filepath) as src:
            block_size = 1024
            for row in range(0, src.height, block_size):
                for col in range(0, src.width, block_size):
                    window = rasterio.windows.Window(
                        col_off=col,
                        row_off=row,
                        width=min(block_size, src.width - col),
                        height=min(block_size, src.height - row),
                    )
                    data = src.read(1, window=window)
                    if -9999 in data:
                        return True
    return False


def reproject(filepath):
    """Reproject grouped aggregation sources into buffered 3857 GeoTIFFs.

    Processing steps:
    1. Parse filepath to extract aggregation tile coords and child zoom
    2. Create temp folder for intermediate files
    3. Skip if reprojection-done marker exists (idempotency)
    4. Load source items from aggregation CSV (grouped by maxzoom, source)
    5. For each priority group (i=0 is highest):
       a. create_virtual_raster: Build VRT mosaicing all group sources
       b. create_warp: Reproject to 3857 with resolution/extent/buffer
       c. translate: Convert VRT to sparse COG with 512px tiling
       d. Check for nodata: If no gaps, skip remaining groups
    6. Write reprojection.json metadata (aggregation_id, buffer_pixels)
    7. Write reprojection-done marker

    The priority ordering ensures highest-resolution sources (maxzoom DESC)
    fill the output first, with lower-res data only used to patch gaps.
    Stopping early when no nodata remains saves unnecessary processing.

    Args:
        filepath: Aggregation CSV path (e.g., aggregation-store/.../12-2130-1459-17-aggregation.csv)
    """
    _, aggregation_id, filename = filepath.split("/")

    z, x, y, child_z = [
        int(a) for a in filename.replace("-aggregation.csv", "").split("-")
    ]

    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)

    tmp_folder = f"aggregation-store/{aggregation_id}/{aggregation_tile.z}-{aggregation_tile.x}-{aggregation_tile.y}-{child_z}-tmp"
    utils.create_folder(tmp_folder)

    metadata_filepath = f"{tmp_folder}/reprojection.json"
    if os.path.isfile(metadata_filepath):
        print(f"reproject {filename} already done...")
        return

    grouped_source_items = utils.get_grouped_source_items(filepath)
    maxzoom = grouped_source_items[0][0]["maxzoom"]
    resolution = get_resolution(maxzoom)

    buffer_pixels = 0
    buffer_3857_rounded = 0
    if len(grouped_source_items) > 1:
        buffer_pixels = int(utils.macrotile_buffer_3857 / resolution)
        buffer_3857_rounded = buffer_pixels * resolution

    for i, source_items in enumerate(grouped_source_items):
        vrt_filepath = create_virtual_raster(tmp_folder, i, source_items)
        zoom = maxzoom
        vrt_3857_filepath = f"{tmp_folder}/{i}-3857.vrt"
        create_warp(
            vrt_filepath, vrt_3857_filepath, zoom, aggregation_tile, buffer_3857_rounded
        )
        out_filepath = f"{tmp_folder}/{i}-3857.tiff"
        translate(vrt_3857_filepath, out_filepath)

        if len(grouped_source_items) > 1 and not contains_nodata_pixels(out_filepath):
            break

    metadata = {
        "buffer_pixels": buffer_pixels,
    }
    with open(metadata_filepath, "w") as f:
        json.dump(metadata, f, indent=2)
