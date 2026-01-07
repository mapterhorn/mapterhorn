"""Orchestrate complete aggregation pipeline: reproject → merge → tile → cleanup.

This module serves as the main entry point for transforming aggregation CSVs
into final PMTiles archives, coordinating three sequential stages:

**Pipeline stages** (per aggregation item):
1. **Reproject** (aggregation_reproject.py):
   - Load source files listed in {z-x-y-childz}-aggregation.csv
   - Group by priority (maxzoom DESC, source ASC)
   - Warp each group to EPSG:3857 with buffering
   - Output: {i}-3857.tiff files (i=0 is highest priority)

2. **Merge** (aggregation_merge.py):
   - Combine priority-ordered rasters via gap-filling
   - Apply Gaussian blur along seams for seamless blending
   - Crop buffer zones after blending
   - Output: Final merged COG

3. **Tile** (aggregation_tile.py):
   - Slice merged raster into 512×512 tiles
   - Encode as Terrarium WebP
   - Package into PMTiles archive
   - Output: pmtiles-store/{parent}/{z-x-y-childz}.pmtiles

4. **Cleanup**: Remove temp folder, write -aggregation.done marker

**Parallel execution**:
- Uses multiprocessing.Pool for concurrent aggregation processing
- Dirty tracking: Only processes changed/new aggregations
- Compares current vs. previous aggregation IDs to identify work
- Typical run: 10-100 aggregations × 4-8 workers = hours vs. days serial

**Output**: PMTiles archives ready for downsampling and bundling.
"""

from glob import glob
import shutil
import os
from multiprocessing import Pool

import aggregation_reproject
import aggregation_merge
import aggregation_tile
import utils


def run(filepath):
    """Process a single aggregation CSV through reprojection, merge, tiling.

    Sequential execution (must complete in order):
    1. aggregation_reproject.reproject(filepath):
       - Reads CSV listing source files and priorities
       - Creates tmp/ folder for intermediate files
       - Warps each priority group to buffered 3857 tiles
       - Writes reprojection.json metadata and reprojection-done marker

    2. aggregation_merge.merge(filepath):
       - Waits for reprojection-done (checked internally)
       - Combines overlapping 3857 tiles with seam blending
       - Fills nodata gaps from lower-priority sources
       - Writes merge-done marker

    3. aggregation_tile.main(filepath):
       - Waits for merge-done (checked internally)
       - Slices merged raster into child tiles
       - Encodes as Terrarium WebP and packages as PMTiles
       - Writes pmtiles-done marker

    4. Cleanup:
       - Remove tmp/ folder (saves ~10-50GB per aggregation)
       - Write -aggregation.done marker for skip logic on reruns

    The done markers enable idempotency: crashed runs can resume from
    the last completed stage without redoing hours of work.

    Args:
        filepath: Path to aggregation CSV (e.g., aggregation-store/.../12-2130-1459-17-aggregation.csv)
    """
    filename = filepath.split("/")[-1]
    item = filename.replace("-aggregation.csv", "")
    print(f"{item} start")
    aggregation_reproject.reproject(filepath)
    aggregation_merge.merge(filepath)
    aggregation_tile.main(filepath)
    tmp_folder = filepath.replace("-aggregation.csv", "-tmp")
    shutil.rmtree(tmp_folder)
    utils.run_command(
        f'touch {filepath.replace("-aggregation.csv", "-aggregation.done")}'
    )
    print(f"{item} end")


def main():
    """Queue aggregation items and process them in parallel pools.

    Job scheduling:
    1. Load aggregation IDs (ULIDs sorted oldest → newest)
    2. Select current aggregation: aggregation_ids[-1]
    3. Identify dirty items:
       - First run (only 1 ID): Process all CSVs in aggregation-store/
       - Subsequent runs: Compare current vs. previous (aggregation_ids[-2])
       - get_dirty_aggregation_filenames: Find new/changed items
    4. Filter out already-done items (check -aggregation.done markers)
    5. Exit early if no work (e.g., re-running after completion)
    6. Launch Pool workers:
       - Automatically uses cpu_count() workers (default)
       - chunksize=1: Distribute items one-at-a-time for load balancing
       - Blocks until all items complete

    The dirty tracking dramatically reduces processing time for incremental
    updates: when only 1 US state changes, only 10-50 aggregations reprocess
    instead of 1000+ globally.

    Typical resource usage:
    - Memory: 2-4GB per worker (GDAL caching + raster buffers)
    - Disk: 10-50GB per worker for temp files
    - CPU: Near 100% (GDAL operations are CPU-bound)
    - Runtime: 2-8 hours for 100-500 dirty aggregations on 8-core machine
    """
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    dirty_filepaths = None
    if len(aggregation_ids) < 2:
        dirty_filepaths = sorted(
            glob(f"aggregation-store/{aggregation_id}/*-aggregation.csv")
        )
    else:
        last_aggregation_id = aggregation_ids[-2]
        dirty_filepaths = [
            f"aggregation-store/{aggregation_id}/{filename}"
            for filename in utils.get_dirty_aggregation_filenames(
                aggregation_id, last_aggregation_id
            )
        ]

    dirty_filepaths = [
        filepath
        for filepath in dirty_filepaths
        if not os.path.isfile(filepath.replace("-aggregation.csv", "-aggregation.done"))
    ]
    if len(dirty_filepaths) == 0:
        print("nothing to do.")
    else:
        print(f"start aggregating {len(dirty_filepaths)} items...")

    argument_tuples = [(dirty_filepath,) for dirty_filepath in dirty_filepaths]
    with Pool() as pool:
        pool.starmap(run, argument_tuples, chunksize=1)


if __name__ == "__main__":
    main()
