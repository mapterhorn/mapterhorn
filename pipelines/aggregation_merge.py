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

def reproject(tmp_folder, aggregation_tile, grouped_source_items):
    maxzoom = grouped_source_items[0][0]['maxzoom']
    resolution = get_resolution(maxzoom)
    buffer_pixels = int(local_config.macrotile_buffer_3857 / resolution)
    buffer_3857_rounded = buffer_pixels * resolution

    for source_items in grouped_source_items:
        create_virtual_raster(tmp_folder, source_items)
        source = source_items[0]['source']
        crs = source_items[0]['crs']
        zoom = maxzoom
        create_warp(tmp_folder, source, crs, zoom, aggregation_tile, buffer_3857_rounded)
        in_filepath = f'{tmp_folder}/{source}-3857.vrt'
        out_filepath = f'{tmp_folder}/{source}-3857.tiff'
        translate(in_filepath, out_filepath)

        if len(grouped_source_items) > 1 and not contains_nodata_pixels(out_filepath):
            break
        
        current = None
        with rasterio.env.Env(GDAL_CACHEMAX=256):
            with rasterio.open(out_filepath) as src: 
                current = src.read(1)

        if merged is None:
            merged = current
            continue
        
        t1 = time.time()
        binary_mask = (merged != -9999).astype('int32')
        print(f'binary_mask done in {time.time() - t1} s...')

        max_pixel_distance = int(0.5 * buffer_3857_rounded / resolution)
        print('max_pixel_distance', max_pixel_distance)

        t1 = time.time()
        reduced = ndimage.binary_erosion(binary_mask, iterations=max_pixel_distance)
        print(f'binary erosion done in {time.time() - t1} s...')

        t1 = time.time()
        alpha_mask = ndimage.uniform_filter(reduced.astype('float32'), int(1.25 * max_pixel_distance), mode='nearest')
        alpha_mask = 3 * alpha_mask ** 2 - 2 * alpha_mask ** 3 # smoothstep with zero derivative at 0 and 1
        alpha_mask = np.where((1 - binary_mask), 0.0, alpha_mask)
        print(f'alpha_mask done in {time.time() - t1} s...')

        t1 = time.time()
        merged = current * (1 - alpha_mask) + merged * alpha_mask
        print(f'merging done in {time.time() - t1} s...')

    print(f'reading {out_filepath}...')
    merged = None
    with rasterio.env.Env(GDAL_CACHEMAX=256):
        with rasterio.open(out_filepath) as src: 
            window = rasterio.windows.Window(
                col_off=buffer_pixels,
                row_off=buffer_pixels,
                width=src.width - 2 * buffer_pixels,
                height=src.height - 2 * buffer_pixels
            )
            merged = src.read(1, window=window)

            print(f'writing merged.tiff...')
            with rasterio.open(
                f'{tmp_folder}/merged.tiff',
                'w',
                driver='GTiff',
                height=merged.shape[0],
                width=merged.shape[1],
                count=1,
                dtype='float32',
            ) as dst:
                dst.write(merged, 1)

def merge(filepath):
    print(f'working on {filepath}')
    _, aggregation_id, filename = filepath.split('/')

    z, x, y = [int(a) for a in filename.replace('.csv', '').split('-')]
    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)

    tmp_folder = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-tmp'

    done_filepath = f'{tmp_folder}/merge-done'
    if os.path.isfile(done_filepath):
        print(f'{filepath} already done...')
        return

    metadata_filepath = f'{tmp_folder}/reprojection.json'
    if not os.path.isfile(metadata_filepath):
        print(f'{filepath} reprojection not done yet...')
        return
    
    num_tiff_files = len(glob(f'{tmp_folder}/*.tiff'))
    if num_tiff_files == 1:
        return

    tiff_filepaths = [f'{tmp_folder}/{i}-3857.tiff' for i in range(num_tiff_files)]

    buffer_pixels = None
    with open(metadata_filepath) as f:
        metadata = json.load(f)
        buffer_pixels = metadata['buffer_pixels']

    merged = None
    with rasterio.env.Env(GDAL_CACHEMAX=256):
        with rasterio.open(tiff_filepaths[0]) as src: 
            merged = src.read(1)

    for tiff_filepath in tiff_filepaths[1:]:
        current = None
        with rasterio.env.Env(GDAL_CACHEMAX=256):
            with rasterio.open(tiff_filepath) as src: 
                current = src.read(1)
        
        t1 = time.time()
        binary_mask = (merged != -9999).astype('int32')
        print(f'binary_mask done in {time.time() - t1} s...')

        max_pixel_distance = int(0.5 * buffer_pixels)

        t1 = time.time()
        reduced = ndimage.binary_erosion(binary_mask, iterations=max_pixel_distance)
        print(f'binary erosion done in {time.time() - t1} s...')

        t1 = time.time()
        alpha_mask = ndimage.uniform_filter(reduced.astype('float32'), int(1.25 * max_pixel_distance), mode='nearest')
        alpha_mask = 3 * alpha_mask ** 2 - 2 * alpha_mask ** 3 # smoothstep with zero derivative at 0 and 1
        alpha_mask = np.where((1 - binary_mask), 0.0, alpha_mask)
        print(f'alpha_mask done in {time.time() - t1} s...')

        t1 = time.time()
        merged = current * (1 - alpha_mask) + merged * alpha_mask
        print(f'merging done in {time.time() - t1} s...')

        if -9999 not in merged:
            break

    print(f'writing merged tiff...')
    with rasterio.open(
        f'{tmp_folder}/{num_tiff_files}-3857.tiff',
        'w',
        driver='GTiff',
        height=merged.shape[0],
        width=merged.shape[1],
        count=1,
        dtype='float32',
    ) as dst:
        dst.write(merged, 1)
    
    command = f'touch {done_filepath}'
    utils.run_command(command)
    
def main():
    remote_aggregation_store = f'{local_config.remote_aggregation_store_path}/'
    local_aggregation_store = f'aggregation-store/'
    utils.create_folder(local_aggregation_store)
    # utils.rsync(src=remote_aggregation_store, dst=local_aggregation_store)

    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    filepaths = sorted(glob(f'aggregation-store/{aggregation_id}/*.csv'))

    # with Pool() as pool:
    #     pool.starmap(reproject, argument_tuples)

    for filepath in filepaths:
        merge(filepath)
        
main()
