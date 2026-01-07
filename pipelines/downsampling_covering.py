"""Compute downsampling coverage CSVs for efficient pyramid overview generation.

This module optimizes downsampling by grouping PMTiles intelligently:

**Problem**: Naive downsampling would create one CSV per parent tile,
resulting in millions of tiny jobs with massive overhead.

**Solution**: Simplify tile coverage to batch-process larger regions:

**Simplification algorithm**:
1. Extract extents: List all aggregation tiles at child_zoom
   (e.g., 13-4260-2918-17-aggregation.csv → extent at z=13)
2. mercantile.simplify: Merge adjacent tiles into larger parents
   (e.g., 4 z=13 tiles → 1 z=12 tile if forming complete quadrant)
3. Clamp zoom range:
   - If simplified tile is at child_zoom: Use z=child_zoom-1 parent
   - If z ≥ child_zoom - 6: Use as-is (good batch size)
   - If z < child_zoom - 6: Subdivide to z=child_zoom-6 (prevent huge jobs)
4. For each simplified extent:
   a. Expand to child tiles at child_zoom
   b. Map children → original extent tiles
   c. Collect unique extent PMTiles needed for downsampling
   d. Write {z-x-y-parentzoom}-downsampling.csv listing inputs

**Output**: Downsampling CSVs with optimal batching:
- Few large CSVs for sparse regions (e.g., oceans)
- Many smaller CSVs for dense regions (e.g., US high-res data)
- Balances parallelism vs. overhead (typical: 100-1000 CSVs per zoom)

**Example**: Child zoom 17 → parent zoom 16 downsampling:
- Input: 10,000 z=13 extent tiles with z=17 data
- Simplified: 2,500 z=11 batch tiles
- Each batch CSV lists 4-16 extent PMTiles as inputs
"""

from glob import glob

import mercantile

import utils


def get_extents_from_coverings(aggregation_id, zoom):
    """List extent tiles at a child zoom for a given aggregation id."""
    extents = []
    filepaths = glob(f"aggregation-store/{aggregation_id}/*-*-*-{zoom}-*.csv")
    for filepath in filepaths:
        filename = filepath.split("/")[-1]
        parts = filename.replace(".csv", "").split("-")
        extent_z, extent_x, extent_y = [int(a) for a in parts[:3]]
        extents.append(mercantile.Tile(x=extent_x, y=extent_y, z=extent_z))
    return extents


def get_tile_to_extent_map(extents, zoom):
    """Map each child tile at zoom to its extent tile."""
    tile_to_extent_map = {}
    for extent in extents:
        for child in mercantile.children(extent, zoom=zoom):
            tile_to_extent_map[child] = extent
    return tile_to_extent_map


def get_simplified_extents(extents, zoom):
    """Simplify extent list to reduce redundant downsampling work.

    Batching strategy:
    1. mercantile.simplify(extents): Merge adjacent tiles
       - If 4 siblings at zoom z all present → parent at z-1
       - Recursively simplifies up to minimal covering set
       - Example: 256 z=15 tiles → 1 z=12 tile (if all siblings present)

    2. Clamp to processable zoom range:
       a. If unlimited.z == zoom:
          - Extent already at target zoom, use parent (z-1)
          - Ensures we're downsampling TO zoom-1 FROM zoom

       b. If unlimited.z ≥ zoom - num_overviews:
          - Within 6 zooms of target (num_overviews=6)
          - Good batch size: ~1-64 input tiles
          - Use as-is

       c. If unlimited.z < zoom - num_overviews:
          - Too coarse (would batch 100s-1000s of tiles)
          - Subdivide to z=zoom-6 for manageable jobs
          - Example: z=5 batch for z=17 data → split to z=11

    The num_overviews=6 limit balances parallelism (more jobs) against
    overhead (job startup cost). Typical result: 100-1000 CSVs per zoom.

    Args:
        extents: List of Tile objects at target zoom
        zoom: Child zoom level being downsampled

    Returns:
        list: Simplified Tile objects for batch processing
    """
    simplified_extents_unlimited = list(mercantile.simplify(extents))
    simplified_extents = []
    for unlimited in simplified_extents_unlimited:
        if unlimited.z == zoom:
            simplified_extents.append(mercantile.parent(unlimited, zoom=zoom - 1))
        elif unlimited.z >= zoom - utils.num_overviews:
            simplified_extents.append(unlimited)
        else:
            simplified_extents += list(
                mercantile.children(unlimited, zoom=zoom - utils.num_overviews)
            )
    return simplified_extents


def main():
    """Generate downsampling covering CSV files per simplified extent."""
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    command = f"rm aggregation-store/{aggregation_id}/*-downsampling.csv"
    utils.run_command(command)

    for child_zoom in reversed(range(1, 32)):
        print(f"\nchild_zoom={child_zoom}")
        print("get extents...")
        extents = get_extents_from_coverings(aggregation_id, child_zoom)

        if len(extents) == 0:
            continue

        print("get tile to extent map...")
        tile_to_extent_map = get_tile_to_extent_map(extents, child_zoom)

        print("get simplified extents...")
        simplified_extents = get_simplified_extents(extents, child_zoom)

        print("iterate over simplified extents...")
        for j, simplified_extent in enumerate(simplified_extents):
            if j % 100 == 0:
                print(f"{j} / {len(simplified_extents)}")
            involved_extents = set({})
            children = list(mercantile.children(simplified_extent, zoom=child_zoom))
            for child in children:
                if child in tile_to_extent_map:
                    involved_extents.add(tile_to_extent_map[child])
            lines = ["filename\n"]
            for involved_extent in involved_extents:
                lines.append(
                    f"{involved_extent.z}-{involved_extent.x}-{involved_extent.y}-{child_zoom}.pmtiles\n"
                )

            out_filepath = f"aggregation-store/{aggregation_id}/{simplified_extent.z}-{simplified_extent.x}-{simplified_extent.y}-{child_zoom - 1}-downsampling.csv"
            with open(out_filepath, "w") as f:
                f.writelines(lines)


if __name__ == "__main__":
    main()
