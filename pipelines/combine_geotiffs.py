from functools import cache

import mercantile

import utils
from utils_geotiff import find_existing_geo_tiffs, get_child_z, get_tile


def find_sources_for_zoom(zoom: int, tile_paths: list[str]) -> list[str]:
    tile_paths_for_zoom = [p for p in tile_paths if get_child_z(p) == zoom]
    paths_by_tile = {get_tile(path): path for path in tile_paths_for_zoom}
    existing_zooms = {t.z for t in paths_by_tile.keys()}
    if len(existing_zooms) == 1:
        # All tiles have the same zoom
        return tile_paths_for_zoom

    # Strategy:
    # - If a tile has data for its full extent, include it
    # - If it only has date for some parts it covers, include the subparts

    @cache
    def is_complete(tile_path: str) -> bool:
        tile = get_tile(tile_path)
        possible_children = mercantile.children(tile)
        existing_children = [c for c in possible_children if c in paths_by_tile]
        if not existing_children:
            return True
        all_present = len(existing_children) == len(possible_children)
        all_complete = all(is_complete(paths_by_tile[c]) for c in existing_children)
        return all_present and all_complete

    return [path for path in tile_paths_for_zoom if is_complete(path)]


def create_combined(zoom, source_paths: list[str]):
    target_prefix = f"geotiff-store/z{zoom}"
    target_path = f"{target_prefix}.vrt"
    input_file_list_path = f"{target_prefix}-sources.txt"
    with open(input_file_list_path, "w") as f:
        f.write("\n".join(source_paths))
    command = f"gdalbuildvrt -overwrite -input_file_list {input_file_list_path} {target_path}"
    out, err = utils.run_command(command)
    if err.strip() != "":
        raise IOError(f"gdalbuildvrt failed for {target_path}:\n{out}\n{err}")
    print(f"Created {target_path}")


def main():
    print("Combining geotiff of same zoom level...")
    paths = find_existing_geo_tiffs()
    zoom_levels = sorted({get_child_z(p) for p in paths})
    print(f"Creating combined file for each level: {zoom_levels}")

    for z in zoom_levels:
        source_paths = find_sources_for_zoom(z, paths)
        print(f"Found {len(source_paths)} sources for zoom {z}")

        create_combined(z, source_paths)


if __name__ == "__main__":
    main()
