from glob import glob
import math
from multiprocessing import Pool
import shutil
import os
import time
import json

import rasterio
import numpy as np
import mercantile
from scipy import ndimage

import local_config
import utils

def get_grouped_source_items(filepath):
    lines = []
    with open(filepath) as f:
        lines = f.readlines()
    lines = lines[1:] # skip header
    line_tuples = []
    for line in lines:
        source, filename, crs, maxzoom = line.strip().split(',')
        maxzoom = int(maxzoom)
        line_tuples.append((
            -maxzoom,
            source,
            crs,
            filename
        ))
    line_tuples = sorted(line_tuples)
    grouped_source_items = []

    first_line_tuple = line_tuples[0]
    last_group_signature = (first_line_tuple[0], first_line_tuple[1], first_line_tuple[2])
    current_group = [{
        'maxzoom': -first_line_tuple[0],
        'source': first_line_tuple[1],
        'crs': first_line_tuple[2],
        'filename': first_line_tuple[3],
    }]
    for line_tuple in line_tuples[1:]:
        current_group_signature = (line_tuple[0], line_tuple[1], line_tuple[2])
        if current_group_signature != last_group_signature:
            grouped_source_items.append(current_group)
            current_group = []
        current_group.append({
            'maxzoom': -line_tuple[0],
            'source': line_tuple[1],
            'crs': line_tuple[2],
            'filename': line_tuple[3],
        })
    grouped_source_items.append(current_group)
    return grouped_source_items

def fetch_source_image(source, filename):    
    remote_file = f'{local_config.remote_source_store_path}/{source}/{filename}'
    local_file = f'source-store/{source}/{filename}'
    command = f'rsync -ahv {remote_file} {local_file}'
    utils.run_command(command)

def create_virtual_raster(filepath, source_items):
    source = source_items[0]['source']
    command = f'gdalbuildvrt -overwrite {filepath}'
    for source_item in source_items:
        command += f' source-store/{source}/{source_item["filename"]}'
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


# def reproject(tmp_folder, aggregation_tile, grouped_source_items):
#     maxzoom = grouped_source_items[0][0]['maxzoom']
#     resolution = get_resolution(maxzoom)
#     buffer_pixels = int(local_config.macrotile_buffer_3857 / resolution)
#     buffer_3857_rounded = buffer_pixels * resolution

#     for source_items in grouped_source_items:
#         create_virtual_raster(tmp_folder, source_items)
#         source = source_items[0]['source']
#         crs = source_items[0]['crs']
#         zoom = maxzoom
#         create_warp(tmp_folder, source, crs, zoom, aggregation_tile, buffer_3857_rounded)
#         in_filepath = f'{tmp_folder}/{source}-3857.vrt'
#         out_filepath = f'{tmp_folder}/{source}-3857.tiff'
#         translate(in_filepath, out_filepath)

#         if len(grouped_source_items) > 1 and not contains_nodata_pixels(out_filepath):
#             break
        
        # current = None
        # with rasterio.env.Env(GDAL_CACHEMAX=256):
        #     with rasterio.open(out_filepath) as src: 
        #         current = src.read(1)

        # if merged is None:
        #     merged = current
        #     continue
        
        # t1 = time.time()
        # binary_mask = (merged != -9999).astype('int32')
        # print(f'binary_mask done in {time.time() - t1} s...')

        # max_pixel_distance = int(0.5 * buffer_3857_rounded / resolution)
        # print('max_pixel_distance', max_pixel_distance)

        # t1 = time.time()
        # reduced = ndimage.binary_erosion(binary_mask, iterations=max_pixel_distance)
        # print(f'binary erosion done in {time.time() - t1} s...')

        # t1 = time.time()
        # alpha_mask = ndimage.uniform_filter(reduced.astype('float32'), int(1.25 * max_pixel_distance), mode='nearest')
        # alpha_mask = 3 * alpha_mask ** 2 - 2 * alpha_mask ** 3 # smoothstep with zero derivative at 0 and 1
        # alpha_mask = np.where((1 - binary_mask), 0.0, alpha_mask)
        # print(f'alpha_mask done in {time.time() - t1} s...')

        # t1 = time.time()
        # merged = current * (1 - alpha_mask) + merged * alpha_mask
        # print(f'merging done in {time.time() - t1} s...')

    # print(f'reading {out_filepath}...')
    # merged = None
    # with rasterio.env.Env(GDAL_CACHEMAX=256):
    #     with rasterio.open(out_filepath) as src: 
    #         window = rasterio.windows.Window(
    #             col_off=buffer_pixels,
    #             row_off=buffer_pixels,
    #             width=src.width - 2 * buffer_pixels,
    #             height=src.height - 2 * buffer_pixels
    #         )
    #         merged = src.read(1, window=window)

    #         print(f'writing merged.tiff...')
    #         with rasterio.open(
    #             f'{tmp_folder}/merged.tiff',
    #             'w',
    #             driver='GTiff',
    #             height=merged.shape[0],
    #             width=merged.shape[1],
    #             count=1,
    #             dtype='float32',
    #         ) as dst:
    #             dst.write(merged, 1)


def reproject(filepath, aggregation_id):
    filename = filepath.split('/')[-1]

    z, x, y = [int(a) for a in filename.replace('.csv', '').split('-')]
    
    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)

    tmp_folder = f'aggregation-store/{aggregation_id}/{aggregation_tile.z}-{aggregation_tile.x}-{aggregation_tile.y}-tmp'
    utils.create_folder(tmp_folder)

    metadata_filepath = f'{tmp_folder}/reprojection.json'
    if os.path.isfile(metadata_filepath):
        print(f'{filepath} already done...')
        return

    grouped_source_items = get_grouped_source_items(filepath)
    maxzoom = grouped_source_items[0][0]['maxzoom']
    resolution = get_resolution(maxzoom)

    buffer_pixels = 0
    buffer_3857_rounded = 0
    if len(grouped_source_items) > 1:
        buffer_pixels = int(local_config.macrotile_buffer_3857 / resolution)
        buffer_3857_rounded = buffer_pixels * resolution

    for i, source_items in enumerate(grouped_source_items):
        vrt_filepath = f'{tmp_folder}/{i}.vrt'
        create_virtual_raster(vrt_filepath, source_items)
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

def main():
    remote_source_store = f'{local_config.remote_source_store_path}/'
    local_source_store = f'source-store/'
    utils.create_folder(local_source_store)
    # utils.rsync(src=remote_source_store, dst=local_source_store, skip_data_files=True)

    remote_aggregation_store = f'{local_config.remote_aggregation_store_path}/'
    local_aggregation_store = f'aggregation-store/'
    utils.create_folder(local_aggregation_store)
    # utils.rsync(src=remote_aggregation_store, dst=local_aggregation_store)

    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    filepaths = sorted(glob(f'aggregation-store/{aggregation_id}/*.csv'))

    argument_tuples = []
    for filepath in filepaths:
        grouped_source_items = get_grouped_source_items(filepath)
        for source_items in grouped_source_items:
            for source_item in source_items:
                fetch_source_image(source_item['source'], source_item['filename'])

        argument_tuples.append((filepath, aggregation_id))
    
    with Pool() as pool:
        pool.starmap(reproject, argument_tuples)

    # for argument_tuple in argument_tuples:
    #     reproject(*argument_tuple)
        
main()
