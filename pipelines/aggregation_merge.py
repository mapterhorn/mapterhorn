"""Merge reprojected tiles for an aggregation into a single buffered GeoTIFF.

This module performs sophisticated DEM mosaic merging with seamless blending:

**Input**: Multiple buffered 3857 GeoTIFFs from aggregation_reproject.py,
priority-ordered by (maxzoom DESC, source), with overlapping buffer zones.

**Merging strategy**:
1. Process output in 512×512 tiles with buffer overlap for context
2. Start with highest-priority (maxzoom) raster as base layer
3. Fill nodata gaps (-9999) from lower-priority rasters in order
4. Detect seam boundaries where rasters meet using binary erosion
5. Apply Gaussian blur along seams to eliminate visible discontinuities
6. Use smooth S-curve weighting (3t² - 2t³) for natural transitions
7. Crop buffer zones before writing to final output

**Boundary smoothing**:
- Binary erosion finds interior edges just inside valid data regions
- Gaussian kernel (σ = buffer_pixels/4 - 1) blurs elevation along seams
- Smoothstep interpolation prevents harsh transitions between sources
- Edge pixels are excluded to avoid artifacts at tile boundaries

**Output**: Single COG with seamlessly blended elevation data ready for
tiling into Terrarium WebP format.
"""

from glob import glob
import os
import time
import json

import rasterio
import numpy as np
from scipy import ndimage

import utils


def merge(filepath):
    """Merge buffer-overlapping reprojected tiles into one COG for an item.

    Processing flow:
    1. Skip if merge-done marker exists (idempotency)
    2. Verify reprojection.json metadata is present
    3. Count available {i}-3857.tiff files (priority-ordered, 0=highest)
    4. Fast path: if only 1 input, skip merging entirely
    5. Otherwise, tile-by-tile merging:
       a. Read 512×512 output window + buffer overlap from all inputs
       b. Start with highest-priority (0-3857.tiff) as base
       c. Copy valid pixels from lower-priority rasters to fill gaps
       d. If gaps filled early, stop reading remaining inputs
       e. Detect seam boundaries via binary erosion
       f. Blur elevation along seams with Gaussian + smoothstep
       g. Crop buffer and write tile to output COG
    6. Write merge-done marker on success

    The buffer overlap ensures seamless blending at tile edges without
    creating visible artifacts. Blurring only happens near seams where
    rasters meet, preserving sharp terrain features elsewhere.

    Args:
        filepath: Path to aggregation CSV (e.g., aggregation-store/.../12-2130-1459-17-aggregation.csv)
    """
    _, aggregation_id, filename = filepath.split("/")

    z, x, y, child_z = [
        int(a) for a in filename.replace("-aggregation.csv", "").split("-")
    ]

    tmp_folder = f"aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-tmp"

    done_filepath = f"{tmp_folder}/merge-done"
    if os.path.isfile(done_filepath):
        print(f"merge {filename} already done...")
        return

    metadata_filepath = f"{tmp_folder}/reprojection.json"
    if not os.path.isfile(metadata_filepath):
        print(f"{filepath} reprojection not done yet...")
        return

    num_tiff_files = len(glob(f"{tmp_folder}/*.tiff"))

    if num_tiff_files == 0:
        raise ValueError(f"failed to read tifs of {filepath}")

    if num_tiff_files == 1:
        command = f"touch {done_filepath}"
        utils.run_command(command)
        return

    tiff_filepaths = [f"{tmp_folder}/{i}-3857.tiff" for i in range(num_tiff_files)]

    buffer_pixels = None
    with open(metadata_filepath) as f:
        metadata = json.load(f)
        buffer_pixels = metadata["buffer_pixels"]

    tile_size = 512
    overlap = buffer_pixels
    with rasterio.env.Env(GDAL_CACHEMAX=256):
        with rasterio.open(tiff_filepaths[0]) as src:
            height = src.height
            width = src.width
            profile = src.profile

            output_path = f"{tmp_folder}/{num_tiff_files}-3857.tiff"
            profile.update(
                tiled=True,
                blockxsize=512,
                blockysize=512,
            )

            with rasterio.open(output_path, "w", **profile) as dst:
                for y in range(0, height, tile_size):
                    for x in range(0, width, tile_size):
                        y_start = max(0, y - overlap)
                        y_end = min(height, y + tile_size + overlap)
                        x_start = max(0, x - overlap)
                        x_end = min(width, x + tile_size + overlap)

                        window = rasterio.windows.Window(
                            x_start, y_start, x_end - x_start, y_end - y_start
                        )

                        merged_tile = None
                        with rasterio.open(tiff_filepaths[0]) as src:
                            merged_tile = src.read(1, window=window)

                        filled_from_start = -9999 not in merged_tile

                        if not filled_from_start:

                            binary_mask = (merged_tile != -9999).astype("int32")
                            eroded = ndimage.binary_erosion(binary_mask)
                            boundary_tile = binary_mask.astype(bool) & ~eroded

                            for tiff_filepath in tiff_filepaths[1:]:

                                with rasterio.open(tiff_filepath) as src:
                                    current_tile = src.read(1, window=window)

                                copy_mask = (merged_tile == -9999) & (
                                    current_tile != -9999
                                )
                                merged_tile[copy_mask] = current_tile[copy_mask]

                                if -9999 not in merged_tile:
                                    break

                                binary_mask = (merged_tile != -9999).astype("int32")
                                eroded = ndimage.binary_erosion(binary_mask)
                                boundary_tile |= binary_mask.astype(bool) & ~eroded

                            boundary_tile[0, :] = 0
                            boundary_tile[-1, :] = 0
                            boundary_tile[:, 0] = 0
                            boundary_tile[:, -1] = 0

                            binary_mask = (merged_tile != -9999).astype("int32")
                            eroded = ndimage.binary_erosion(binary_mask)
                            boundary_tile &= eroded.astype(bool)

                            if 1 in boundary_tile:
                                merged_tile[merged_tile == -9999] = 0
                                truncate = 4
                                sigma = int(overlap / truncate) - 1
                                boundary_tile_blurred = ndimage.gaussian_filter(
                                    boundary_tile.astype(float),
                                    sigma=sigma,
                                    truncate=truncate,
                                )
                                boundary_tile_blurred /= 1.0 / (
                                    np.sqrt(2 * np.pi) * sigma
                                )
                                boundary_tile_blurred = np.clip(
                                    boundary_tile_blurred, 0, 1
                                )
                                boundary_tile_blurred = (
                                    3 * boundary_tile_blurred**2
                                    - 2 * boundary_tile_blurred**3
                                )
                                merged_tile_blurred = ndimage.gaussian_filter(
                                    merged_tile, sigma=sigma, truncate=truncate
                                )
                                merged_tile = (
                                    boundary_tile_blurred * merged_tile_blurred
                                    + (1 - boundary_tile_blurred) * merged_tile
                                )

                        crop_y_start = overlap if y > 0 else 0
                        crop_y_end = merged_tile.shape[0] - (
                            overlap if y_end < height else 0
                        )
                        crop_x_start = overlap if x > 0 else 0
                        crop_x_end = merged_tile.shape[1] - (
                            overlap if x_end < width else 0
                        )

                        output_window = rasterio.windows.Window(
                            x, y, crop_x_end - crop_x_start, crop_y_end - crop_y_start
                        )
                        dst.write(
                            merged_tile[
                                crop_y_start:crop_y_end, crop_x_start:crop_x_end
                            ],
                            1,
                            window=output_window,
                        )

    command = f"touch {done_filepath}"
    utils.run_command(command)


if __name__ == "__main__":
    filepath = (
        "aggregation-store/01K7M3DMZF4RFVFYDWN9KF1Q4N/12-2130-1459-17-aggregation.csv"
    )
    tic = time.time()
    merge(filepath)
