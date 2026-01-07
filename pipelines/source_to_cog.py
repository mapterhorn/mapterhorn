"""Convert diverse raster formats to Cloud Optimized GeoTIFFs with compression.

This module normalizes raw DEM data from various sources (USGS, NASA, etc.)
into a standardized COG format for efficient pipeline processing:

**Supported input formats**:
- GeoTIFF (.tif, .TIF, .tiff) - Already spatial, just optimize structure
- ASCII Grid (.asc, .ASC) - ESRI ASCII raster format
- XYZ (.xyz) - Space-delimited x y z point clouds
- Text (.txt) - Generic delimited elevation data

**Conversion process**:
1. Extension normalization: Convert all variants to lowercase .tif
2. LERC compression: Lossy Elevation Raster Compression with 0.001m error
   (imperceptible quality loss, ~10-30× size reduction vs. uncompressed)
3. 512×512 internal tiling: Enables efficient partial reads
4. Sparse optimization: Omits fully-nodata tiles from storage
5. BigTIFF: Supports >4GB files common for high-res regional datasets
6. No built-in overviews: Pyramids generated later in downsampling stage

**Multiprocessing**: Converts files in parallel using Pool for faster
batch processing of large source collections (e.g., 1000+ USGS tiles).

**Output**: Normalized source-store/{source}/*.tif files ready for:
- CRS verification (source_set_crs.py)
- Nodata value setting (source_set_nodata.py)
- Bounds computation (source_bounds.py)
"""

from glob import glob
import sys
from multiprocessing import Pool
import utils

SILENT = False


def to_cog(filepath):
    """Convert a single file to COG, normalizing extensions along the way.

    Handles various input scenarios:
    1. .tif/.TIF/.tiff: Rename to .bak, convert .bak → .tif (optimizes structure)
    2. .xyz/.asc/.ASC/.txt: Convert directly to .tif (GDAL auto-detects format)

    GDAL translation settings:
    - GDAL_CACHEMAX=512: 512MB memory buffer for large files
    - BLOCKSIZE=512: Internal tile size matching aggregation pipeline
    - OVERVIEWS=NONE: Skip pyramids (done in downsampling)
    - SPARSE_OK=YES: Omit nodata tiles (saves space for partial coverage)
    - BIGTIFF=YES: Support >4GB files
    - COMPRESS=LERC: Lossy compression with MAX_Z_ERROR=0.001m

    The LERC compression achieves excellent ratios (10-30×) with negligible
    quality impact: 1mm error is far below source accuracy for most DEMs
    (SRTM=16m, ASTER=30m, USGS 1/3"=7-10m vertical RMSE).

    Args:
        filepath: Input raster path (any supported format)
    """
    filepath_in = None
    filepath_out = None
    if filepath.endswith(".tif"):
        utils.run_command(f"mv {filepath} {filepath}.bak", silent=SILENT)
        filepath_in = f"{filepath}.bak"
        filepath_out = filepath
    elif filepath.endswith(".TIF"):
        utils.run_command(f"mv {filepath} {filepath}.bak", silent=SILENT)
        filepath_in = f"{filepath}.bak"
        filepath_out = filepath.replace(".TIF", ".tif")
    elif filepath.endswith(".tiff"):
        utils.run_command(f"mv {filepath} {filepath}.bak", silent=SILENT)
        filepath_in = f"{filepath}.bak"
        filepath_out = filepath.replace(".tiff", ".tif")
    elif filepath.endswith(".xyz"):
        filepath_in = filepath
        filepath_out = filepath.replace(".xyz", ".tif")
    elif filepath.endswith(".asc"):
        filepath_in = filepath
        filepath_out = filepath.replace(".asc", ".tif")
    elif filepath.endswith(".ASC"):
        filepath_in = filepath
        filepath_out = filepath.replace(".ASC", ".tif")
    elif filepath.endswith(".txt"):
        filepath_in = filepath
        filepath_out = filepath.replace(".txt", ".tif")

    utils.run_command(
        f'GDAL_CACHEMAX=512 gdal_translate -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath_in}" "{filepath_out}"',
        silent=SILENT,
    )
    utils.run_command(f'rm "{filepath_in}"', silent=SILENT)


def main():
    """Convert all supported raster files for a source into COGs."""
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f"converting to cog for source={source}...")
    else:
        print("source argument missing")
        exit()

    filepaths = []
    filepaths += glob(f"source-store/{source}/*.tif")
    filepaths += glob(f"source-store/{source}/*.TIF")
    filepaths += glob(f"source-store/{source}/*.tiff")
    filepaths += glob(f"source-store/{source}/*.xyz")
    filepaths += glob(f"source-store/{source}/*.asc")
    filepaths += glob(f"source-store/{source}/*.ASC")
    filepaths += glob(f"source-store/{source}/*.txt")

    filepaths = [(filepath,) for filepath in sorted(filepaths)]

    print(f"num files: {len(filepaths)}")
    with Pool() as pool:
        pool.starmap(to_cog, filepaths)


if __name__ == "__main__":
    main()
