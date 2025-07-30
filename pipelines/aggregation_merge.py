from glob import glob
from multiprocessing import Pool
import os
import time
import json

import rasterio
import numpy as np
from scipy import ndimage

import utils

def merge(filepath):
    print(f'merging {filepath}...')
    _, aggregation_id, filename = filepath.split('/')

    z, x, y, child_z = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]

    tmp_folder = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-tmp'

    done_filepath = f'{tmp_folder}/merge-done'
    if os.path.isfile(done_filepath):
        print(f'merge {filename} already done...')
        return

    metadata_filepath = f'{tmp_folder}/reprojection.json'
    if not os.path.isfile(metadata_filepath):
        print(f'{filepath} reprojection not done yet...')
        return
    
    num_tiff_files = len(glob(f'{tmp_folder}/*.tiff'))
    if num_tiff_files == 1:
        print('single file...')
        command = f'touch {done_filepath}'
        utils.run_command(command)
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

    t1 = time.time()
    with rasterio.open(
        f'{tmp_folder}/{num_tiff_files}-3857.tiff',
        'w',
        driver='GTiff',
        height=merged.shape[0],
        width=merged.shape[1],
        count=1,
        dtype='float32',
        tiled=True,
        blockxsize=512,
        blockysize=512,
    ) as dst:
        dst.write(merged, 1)
    print(f'writing done in {time.time() - t1} s...')
    
    command = f'touch {done_filepath}'
    utils.run_command(command)
    
def main(filepaths):
    # needs ~30 GB per thread
    pool_size = 2
    with Pool(pool_size) as pool:
        pool.starmap(merge, [(filepath,) for filepath in filepaths])
