import os
import shutil
from multiprocessing import Pool

import mercantile
from mercantile import Tile
from rasterio.crs import defaultdict

import utils
import aggregation_reproject
from utils_geotiff import find_existing_geo_tiffs, get_child_z, get_tile


def new_tile_path(tile: Tile, source_paths: list[str]) -> str:
    child_z = determine_new_child_z(tile, source_paths)
    folder = utils.get_geotiff_folder(tile.x, tile.y, tile.z)
    return f"{folder}/{tile.z}-{tile.x}-{tile.y}-{child_z}.tif"


def determine_new_child_z(tile: Tile, source_paths: list[str]) -> int:
    source_max_child_z = max(get_child_z(path) for path in source_paths)
    return min(source_max_child_z, tile.z + 6)


def find_tiles_to_create(tile_paths: list[str]) -> list[tuple[Tile, list[str]]]:
    tiles = {get_tile(path): path for path in tile_paths}
    max_z = max(tile.z for tile in tiles.keys())

    tiles_to_create: list[tuple[Tile, list[str]]] = []
    for z in range(max_z, 0, -1):
        tiles_to_create_for_zoom = defaultdict(list)
        for tile, path in tiles.items():
            if tile.z != z:
                continue
            parent = mercantile.parent(tile)
            if parent not in tiles:
                tiles_to_create_for_zoom[parent].append(path)

        new_tiles = {
            tile: new_tile_path(tile, source_paths)
            for tile, source_paths in tiles_to_create_for_zoom.items()
        }

        tiles |= new_tiles
        tiles_to_create += tiles_to_create_for_zoom.items()

    return tiles_to_create


def create_tile(tile: Tile, source_paths: list[str]) -> None:
    print(f"started {tile}")
    child_z = determine_new_child_z(tile, source_paths)
    target_path = new_tile_path(tile, source_paths)
    target_prefix = target_path.removesuffix(".tif")

    resolution = aggregation_reproject.get_resolution(child_z)
    left, bottom, right, top = mercantile.xy_bounds(tile)

    mosaic_path = f"{target_prefix}.mosaic.vrt"
    command = f"gdal raster mosaic {' '.join(source_paths)} {mosaic_path}"
    command += " --resolution highest"
    command += " --overwrite"
    command += " --src-nodata -9999 --dst-nodata -9999"
    out, err = utils.run_command(command)
    if err.strip() != "":
        print(f"gdal raster mosaic failed for {target_path}:\n{out}\n{err}")
        return

    target_path_tmp = f"{target_prefix}.tmp.tif"
    command = f"gdalwarp {mosaic_path} {target_path_tmp}"
    command += " -r average"
    command += f" -te {left} {bottom} {right} {top}"
    command += f" -tr {resolution} {resolution}"
    command += " -of COG"
    command += " -co BIGTIFF=IF_NEEDED -co ADD_ALPHA=YES -co OVERVIEWS=NONE"
    command += " -co BLOCKSIZE=512 -co COMPRESS=LERC -co MAX_Z_ERROR=0.001"
    out, err = utils.run_command(command)
    if err.strip() != "":
        raise IOError(f"gdalwarp failed for {target_path}:\n{out}\n{err}")

    os.remove(mosaic_path)
    shutil.move(target_path_tmp, target_path)
    print(f"finished {target_path}")


def main():
    print("Computing merged and downsampled tiles to create...")
    tiles = find_existing_geo_tiffs()
    tiles_to_create = find_tiles_to_create(tiles)

    print(f"Creating {len(tiles_to_create)} tiles by merging and downsampling.")
    levels = sorted({t.z for t, _ in tiles_to_create}, reverse=True)
    for level in levels:
        argument_tuples = [(tile, p) for tile, p in tiles_to_create if tile.z == level]
        print(f"Processing z={level} with {len(argument_tuples)} tiles.")
        with Pool(processes=16) as pool:
            pool.starmap(create_tile, argument_tuples, chunksize=1)


if __name__ == "__main__":
    main()
