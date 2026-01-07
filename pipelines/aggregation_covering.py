"""Compute macrotile coverage and aggregation groupings for source rasters.

This module orchestrates the first stage of DEM tile aggregation by:
1. Reading source raster bounds from bounds.csv files in source-store/
2. Computing which Web Mercator macrotiles (z=12) each source intersects
3. Determining appropriate maxzoom levels based on source resolution
4. Grouping macrotiles by their (source, maxzoom) combinations
5. Identifying aggregation tiles that can batch-process groups efficiently
6. Writing aggregation CSV manifests describing inputs for each tile

The output is an aggregation-store/{ULID}/ directory containing CSV files
that drive subsequent reprojection, merging, and tiling stages. Each CSV
lists source files contributing to one aggregation tile, enabling parallel
processing while respecting source priority and resolution requirements.

**Source Priority System**:
When multiple sources overlap, priority is determined by:
1. **Resolution (maxzoom DESC)**: Higher-resolution sources take precedence
   - Example: USGS 1/3" (z=17) beats GLO-30 (z=12) in overlap areas
2. **Source name (ASC)**: Alphabetical tiebreaker for same-resolution sources
   - Example: "alos-30" beats "glo-30" when both are z=12

This ensures the highest-quality data fills the output first, with
lower-resolution sources only patching gaps. The priority order is
enforced in utils.get_grouped_source_items() via sorting by
(-maxzoom, source).

Key concepts:
- Macrotiles: Fixed z=12 tiles used as atomic units of coverage
- Aggregation tiles: Variable-zoom tiles that group macrotiles with
  identical source compositions to minimize redundant processing
- Anti-meridian handling: Properly splits/tests bboxes crossing ±180°
- Buffering: Adds margin around sources to enable seamless merging
"""

from glob import glob

import mercantile
from ulid import ULID

import utils


def get_mercator_resolutions(minzoom, maxzoom):
    """Return per-zoom pixel resolutions (meters per pixel) for 512px tiles."""
    resolutions = []
    for z in range(minzoom, maxzoom + 1):
        tile = mercantile.Tile(x=0, y=0, z=z)
        bounds = mercantile.xy_bounds(tile)
        resolutions.append((bounds.right - bounds.left) / 512)
    return resolutions


def bounds_intersect_no_anitmeridian_crossing(a, b):
    """Return True if two non-wrapping bounding boxes intersect."""
    left_a, bottom_a, right_a, top_a = a
    left_b, bottom_b, right_b, top_b = b
    dont_intersect = False
    dont_intersect |= right_a <= left_b
    dont_intersect |= right_b <= left_a
    dont_intersect |= top_a <= bottom_b
    dont_intersect |= top_b <= bottom_a
    return not dont_intersect


def split_at_antimeridian(bbox):
    """Split a bbox that crosses the anti-meridian into west/east pieces."""
    left, bottom, right, top = bbox
    if left < right:
        return [bbox]
    bbox_1 = (left, bottom, utils.X_MAX_3857, top)
    bbox_2 = (utils.X_MIN_3857, bottom, right, top)
    return [bbox_1, bbox_2]


def bounds_intersect(a, b):
    """Check bbox intersection while accounting for anti-meridian wrapping."""
    for aa in split_at_antimeridian(a):
        for bb in split_at_antimeridian(b):
            if bounds_intersect_no_anitmeridian_crossing(aa, bb):
                return True
    return False


def get_intersecting_tiles_dfs(bounds, tile, zoom):
    """Recursively collect tiles that intersect bounds up to the target zoom."""
    tile_bounds = mercantile.xy_bounds(tile)
    if not bounds_intersect(bounds, tile_bounds):
        return []
    if tile.z == zoom:
        return [tile]
    result = []
    for child in mercantile.children(tile, zoom=tile.z + 1):
        result += get_intersecting_tiles_dfs(bounds, child, zoom)
    return result


def get_macrotile_map():
    """Build mapping of macrotiles to contributing sources and maxzoom hints.

    Algorithm:
    1. Load bounds.csv from each source in source-store/
    2. For each source raster:
       a. Transform bounds to EPSG:3857 (done previously in source_bounds.py)
       b. Apply 2× macrotile buffer (300m) to ensure overlap for blending
       c. Use DFS to find all z=12 macrotiles intersecting buffered bounds
       d. Compute maxzoom where Mercator resolution beats source pixel size
       e. Enforce minimum maxzoom of 12 (macrotile_z) for consistent tiling
       f. Record (filename, maxzoom) for each intersecting macrotile
    3. Return dict: {(tile.x, tile.y): {'sources': {source: [items]}}}

    The maxzoom calculation ensures we don't upsample low-res sources beyond
    their native resolution, while the z=12 minimum guarantees adequate detail
    for global datasets like GLO-30.

    Returns:
        dict: Macrotile map with structure {(x,y): {'sources': {...}}}
    """
    macrotile_map = {}
    filepaths = sorted(glob(f"source-store/*/bounds.csv"))
    mercator_resolutions = get_mercator_resolutions(0, 32)
    for filepath in filepaths:
        print(f"reading {filepath}...")
        source = filepath.split("/")[1]
        with open(filepath) as f:
            f.readline()  # skip header
            line = f.readline().strip()
            while line != "":
                filename, left, bottom, right, top, width, height = line.split(",")
                width, height = [int(a) for a in [width, height]]
                left, bottom, right, top = [
                    float(a) for a in [left, bottom, right, top]
                ]

                multiplier = 2
                buffer = multiplier * utils.macrotile_buffer_3857
                buffered_bounds = (
                    left - buffer,
                    bottom - buffer,
                    right + buffer,
                    top + buffer,
                )

                tiles = get_intersecting_tiles_dfs(
                    buffered_bounds, mercantile.Tile(x=0, y=0, z=0), utils.macrotile_z
                )

                maxzoom = get_smallest_overzoom(
                    left, bottom, right, top, width, height, mercator_resolutions
                )

                # Use at least a maxzoom of 12 (macrotile_z).
                # Note that glo30 does not everywhere give a maxzoom of 12. Examples:
                # S79 native maxzoom 9
                # N66 native maxzoom 10
                # N50 native maxzoom 11
                # N49 native maxzoom 12 (group has only ~30 percent of total macrotiles)
                # Use gdal warp with cubicspline when maxzoom is 12
                maxzoom = max(maxzoom, utils.macrotile_z)

                for tile in tiles:
                    if (tile.x, tile.y) not in macrotile_map:
                        macrotile_map[(tile.x, tile.y)] = {"sources": {}}
                    if source not in macrotile_map[(tile.x, tile.y)]["sources"]:
                        macrotile_map[(tile.x, tile.y)]["sources"][source] = []
                    macrotile_map[(tile.x, tile.y)]["sources"][source].append(
                        {
                            "filename": filename,
                            "maxzoom": maxzoom,
                        }
                    )
                line = f.readline().strip()

    return macrotile_map


def get_smallest_overzoom(
    left, bottom, right, top, width, height, mercator_resolutions
):
    """Find lowest zoom where Mercator resolution beats source pixel resolution."""
    horizontal_resolution = (
        (right - left) / width if left < right else (left - right) / width
    )
    vertical_resolution = (top - bottom) / height

    for z in range(len(mercator_resolutions)):
        if (
            mercator_resolutions[z] < horizontal_resolution
            and mercator_resolutions[z] < vertical_resolution
        ):
            return z
    raise ValueError(
        f"No overzoom found. (left, bottom, right, top, width, height) = {(left, bottom, right, top, width, height)}"
    )


def add_group_ids(macrotile_map):
    """Attach deterministic group ids based on unique (source, maxzoom) pairs."""
    for tile_tuple in macrotile_map:
        group_id_parts = set({})
        for source in macrotile_map[tile_tuple]["sources"]:
            for source_item in macrotile_map[tile_tuple]["sources"][source]:
                group_id_parts.add((source, source_item["maxzoom"]))
        group_id = tuple(sorted(list(group_id_parts)))
        macrotile_map[tile_tuple]["group_id"] = group_id


def get_aggregation_tiles_dfs(candidate, macrotile_map):
    """Depth-first search for aggregation tiles that can share source groups."""
    if candidate.z == utils.macrotile_z:
        return [candidate]
    macrotiles = list(mercantile.children(candidate, zoom=utils.macrotile_z))
    group_ids = set({})
    for macrotile in macrotiles:
        tile_tuple = (macrotile.x, macrotile.y)
        if tile_tuple in macrotile_map:
            group_ids.add(macrotile_map[tile_tuple]["group_id"])
    if len(group_ids) == 0:
        return []
    if len(group_ids) == 1:
        group_id = list(group_ids)[0]
        maxzoom = 0
        for part in group_id:
            maxzoom = max(maxzoom, part[1])
        if candidate.z >= maxzoom - utils.num_overviews:
            return [candidate]
    result = []
    for child in mercantile.children(candidate, zoom=candidate.z + 1):
        result += get_aggregation_tiles_dfs(child, macrotile_map)
    return result


def get_aggregation_tiles(macrotile_map):
    """Return aggregation tiles that need processing based on macrotile groups."""
    candidates = set({})
    for tile_tuple in macrotile_map.keys():
        candidates.add(
            mercantile.parent(
                mercantile.Tile(x=tile_tuple[0], y=tile_tuple[1], z=utils.macrotile_z),
                zoom=utils.macrotile_z - utils.num_overviews,
            )
        )
    aggregation_tiles = []
    for candidate in candidates:
        aggregation_tiles += get_aggregation_tiles_dfs(candidate, macrotile_map)
    return aggregation_tiles


def write_aggregation_items(macrotile_map, aggregation_tiles, aggregation_id):
    """Write CSV records describing source inputs for each aggregation tile."""
    folder = f"aggregation-store/{aggregation_id}"
    utils.create_folder(folder)
    for aggregation_tile in aggregation_tiles:
        macrotiles = list(mercantile.children(aggregation_tile, zoom=utils.macrotile_z))
        lines = ["source,filename,maxzoom\n"]
        line_tuples = set({})
        child_z = 0
        for macrotile in macrotiles:
            tile_tuple = (macrotile.x, macrotile.y)
            if tile_tuple not in macrotile_map:
                continue
            for source in macrotile_map[tile_tuple]["sources"]:
                for source_item in macrotile_map[tile_tuple]["sources"][source]:
                    line_tuples.add(
                        (source, source_item["filename"], str(source_item["maxzoom"]))
                    )
                    child_z = max(child_z, source_item["maxzoom"])
        if len(line_tuples) == 0:
            continue
        line_tuples = sorted(list(line_tuples))
        for line_tuple in line_tuples:
            lines.append(f'{",".join(line_tuple)}\n')
        with open(
            f"{folder}/{aggregation_tile.z}-{aggregation_tile.x}-{aggregation_tile.y}-{child_z}-aggregation.csv",
            "w",
        ) as f:
            f.writelines(lines)


def main():
    """Generate aggregation-store entries from source bounds tables."""

    print("get_macrotile_map...")
    macrotile_map = get_macrotile_map()

    print("add group ids...")
    add_group_ids(macrotile_map)

    print("get aggregation tiles...")
    aggregation_tiles = get_aggregation_tiles(macrotile_map)

    aggregation_id = str(ULID())
    utils.create_folder(f"aggregation-store/{aggregation_id}")

    print("write aggregation items...")
    write_aggregation_items(macrotile_map, aggregation_tiles, aggregation_id)


if __name__ == "__main__":
    main()
