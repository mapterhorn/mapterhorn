from multiprocessing import Pool
import json
import os

import rasterio
import mercantile

import utils

def create_virtual_raster(filepath, source_items, tmp_source_folder):
    source = source_items[0]['source']
    command = f'gdalbuildvrt -overwrite {filepath}'
    for source_item in source_items:
        command += f' {tmp_source_folder}/{source}/{source_item["filename"]}'
    utils.run_command(command)

def get_resolution(zoom):
    tile = mercantile.Tile(x=0, y=0, z=zoom)
    bounds = mercantile.xy_bounds(tile)
    return (bounds.right - bounds.left) / 512

def create_warp(vrt_filepath, vrt_3857_filepath, crs, zoom, aggregation_tile, buffer):
    left, bottom, right, top = mercantile.xy_bounds(aggregation_tile)
    left -= buffer
    bottom -= buffer
    right += buffer
    top += buffer
    resolution = get_resolution(zoom)
    command = f'gdalwarp -of vrt -multi -wo NUM_THREADS=ALL_CPUS -overwrite '
    command += f'-s_srs {crs} -t_srs EPSG:3857 '
    command += f'-tr {resolution} {resolution} '
    command += f'-te {left} {bottom} {right} {top} '
    command += f'-r cubicspline '
    command += f'{vrt_filepath} {vrt_3857_filepath}'
    utils.run_command(command)

def translate(in_filepath, out_filepath):
    command = f'GDAL_CACHEMAX=512 gdal_translate -of COG -co NUM_THREADS=ALL_CPUS '
    command += f'--config GDAL_NUM_THREADS all_cpus '
    command += f'-co BIGTIFF=IF_NEEDED -co ADD_ALPHA=YES -co OVERVIEWS=NONE '
    command += f'-co SPARSE_OK=YES -co BLOCKSIZE=512 -co COMPRESS=NONE '
    command += f'{in_filepath} '
    command += f'{out_filepath}'
    utils.run_command(command)

def contains_nodata_pixels(filepath):
    with rasterio.env.Env(GDAL_CACHEMAX=64):
        with rasterio.open(filepath) as src:
            block_size = 1024
            for row in range(0, src.height, block_size):
                for col in range(0, src.width, block_size):
                    window = rasterio.windows.Window(
                        col_off=col,
                        row_off=row,
                        width=min(block_size, src.width - col),
                        height=min(block_size, src.height - row)
                    )
                    data = src.read(1, window=window)
                    if -9999 in data:
                        return True
    return False

def reproject(filepath, aggregation_id):
    print(f'reprojecting {filepath}...')
    filename = filepath.split('/')[-1]

    z, x, y, child_z = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]
    
    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)

    tmp_folder = f'aggregation-store/{aggregation_id}/{aggregation_tile.z}-{aggregation_tile.x}-{aggregation_tile.y}-{child_z}-tmp'
    utils.create_folder(tmp_folder)

    metadata_filepath = f'{tmp_folder}/reprojection.json'
    if os.path.isfile(metadata_filepath):
        print(f'reproject {filename} already done...')
        return

    grouped_source_items = utils.get_grouped_source_items(filepath)
    maxzoom = grouped_source_items[0][0]['maxzoom']
    resolution = get_resolution(maxzoom)

    buffer_pixels = 0
    buffer_3857_rounded = 0
    if len(grouped_source_items) > 1:
        buffer_pixels = int(utils.macrotile_buffer_3857 / resolution)
        buffer_3857_rounded = buffer_pixels * resolution

    tmp_source_folder = f'aggregation-store/{aggregation_id}/tmp-sources'
    for i, source_items in enumerate(grouped_source_items):
        vrt_filepath = f'{tmp_folder}/{i}.vrt'
        create_virtual_raster(vrt_filepath, source_items, tmp_source_folder)
        crs = source_items[0]['crs']
        zoom = maxzoom
        vrt_3857_filepath = f'{tmp_folder}/{i}-3857.vrt'
        create_warp(vrt_filepath, vrt_3857_filepath, crs, zoom, aggregation_tile, buffer_3857_rounded)
        out_filepath = f'{tmp_folder}/{i}-3857.tiff'
        translate(vrt_3857_filepath, out_filepath)

        if len(grouped_source_items) > 1 and not contains_nodata_pixels(out_filepath):
            break
    
    metadata = {
        'buffer_pixels': buffer_pixels,
    }
    with open(metadata_filepath, 'w') as f:
        json.dump(metadata, f, indent=2)

def main(filepaths):
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    argument_tuples = []
    for filepath in filepaths:
        argument_tuples.append((filepath, aggregation_id))
    
    with Pool() as pool:
        pool.starmap(reproject, argument_tuples)
