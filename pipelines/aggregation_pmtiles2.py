from glob import glob
import math
from multiprocessing import Pool
import shutil
import os
import json
import time

import mercantile
import numpy as np

from PIL import Image
import cv2
import imagecodecs

import rasterio
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

import local_config
import utils

def create_tiles(tmp_folder, aggregation_tile, tiff_filepath, buffer_pixels):
    base_x = aggregation_tile.x
    base_y = aggregation_tile.y
    base_z = aggregation_tile.z

    max_z = None
    with rasterio.open(tiff_filepath) as src:
        assert len(src.block_shapes) >= 1
        assert src.block_shapes[0] == (512, 512)
        horizontal_block_count = (src.width - 2 * buffer_pixels) / 512
        assert math.floor(horizontal_block_count) == horizontal_block_count
        max_z = base_z + int(math.log2(horizontal_block_count))
    argument_tuples = []
    for z in [max_z]: # range(local_config.macrotile_z, max_z + 1):
        x_min = base_x * 2 ** (z - base_z)
        y_min = base_y * 2 ** (z - base_z)
        factor = 2 ** (max_z - z)
        for i, x in enumerate(range(x_min, x_min + 2 ** (z - base_z))):
            for j, y in enumerate(range(y_min, y_min + 2 ** (z - base_z))):
                out_filepath = f'{tmp_folder}/{z}-{x}-{y}.png'
                argument_tuples.append((i, j, factor, tiff_filepath, out_filepath, buffer_pixels))
    
    with Pool() as pool:
        pool.starmap(create_tile, argument_tuples)

def create_tile(i, j, factor, tiff_filepath, out_filepath, buffer_pixels):
    t1 = time.time()
    col_start = i * 512 * factor + buffer_pixels
    col_end = (i + 1) * 512 * factor + buffer_pixels
    row_start = j * 512 * factor + buffer_pixels
    row_end = (j + 1) * 512 * factor + buffer_pixels
    window = rasterio.windows.Window(
        col_off=col_start,
        row_off=row_start,
        width=col_end - col_start,
        height=row_end - row_start
    )
    subdata = None
    with rasterio.open(tiff_filepath) as src: 
        subdata = src.read(1, window=window, out_shape=(512, 512))
    t_read = time.time() - t1
    t1 = time.time()
    subdata[subdata == -9999] = 0
    subdata += 32768
    rows, cols = 512, 512
    rgb = np.zeros((rows, cols, 3), dtype=np.uint8)
    rgb[..., 0] = subdata // 256
    rgb[..., 1] = np.floor(subdata % 256)
    rgb[..., 2] = np.floor((subdata - np.floor(subdata)) * 256)
    t_calc = time.time() - t1
    t1 = time.time()

    # image = Image.fromarray(rgb, mode='RGB')
    # image.save(out_filepath, format='WEBP', lossless=True)

    image_bytes = imagecodecs.png_encode(rgb)
    with open(out_filepath, 'wb') as f:
        f.write(image_bytes)
    
    # command = f'cwebp -lossless {png_filepath} -o {out_filepath}'
    # utils.run_command(command)

    t_save = time.time() - t1
    # print(f'{factor} read: {int(1000*t_read)} ms. calc: {int(1000 * t_calc)} ms. save {int(1000*t_save)} ms.')

def create_archive(tmp_folder, out_filepath, aggregation_tile):
    with open(out_filepath, 'wb') as f1:
        writer = Writer(f1)
        max_z = 0
        for filepath in glob(f'{tmp_folder}/*.png'): # png vs webp
            filename = filepath.split('/')[-1]
            z, x, y = [int(a) for a in filename.replace('.png', '').split('-')] # png vs webp
            if z > max_z:
                max_z = z
            tile_id = zxy_to_tileid(z=z, x=x, y=y)
            with open(filepath, 'rb') as f2:
                writer.write_tile(tile_id, f2.read())

        bounds = mercantile.bounds(aggregation_tile)
        min_lon_e7 = int(bounds.west * 1e7)
        min_lat_e7 = int(bounds.south * 1e7)
        max_lon_e7 = int(bounds.east * 1e7)
        max_lat_e7 = int(bounds.north * 1e7)

        writer.finalize(
            {
                'tile_type': TileType.WEBP, # png vs webp
                'tile_compression': Compression.NONE,
                'min_zoom': local_config.macrotile_z,
                'max_zoom': max_z,
                'min_lon_e7': min_lon_e7,
                'min_lat_e7': min_lat_e7,
                'max_lon_e7': max_lon_e7,
                'max_lat_e7': max_lat_e7,
                'center_zoom': int(0.5 * (local_config.macrotile_z + max_z)),
                'center_lon_e7': int(0.5 * (min_lon_e7 + max_lon_e7)),
                'center_lat_e7': int(0.5 * (min_lat_e7 + max_lat_e7)),
            },
            {
                'attribution': '<a href="https://github.com/mapterhorn/mapterhorn">© Mapterhorn</a>'
            },
        )

def main(filepaths):
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    for j, filepath in enumerate(filepaths):
        print(f'working on {filepath}. {j + 1} / {len(filepaths)}.')
        filename = filepath.split('/')[-1]

        z, x, y, child_z = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]

        tmp_folder = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-tmp'

        aggregation_tile = mercantile.Tile(x=x, y=y, z=z)
        out_filepath = filepath.replace('-aggregation.csv', '.pmtiles')

        if os.path.isfile(out_filepath):
            print(f'{filepath} already done...')
            continue

        merge_done = os.path.isfile(f'{tmp_folder}/merge-done')
        if not merge_done:
            print('merge not done yet...')
            continue

        buffer_pixels = None
        with open(f'{tmp_folder}/reprojection.json') as f:
            metadata = json.load(f)
            buffer_pixels = metadata['buffer_pixels']

        num_tiff_files = len(glob(f'{tmp_folder}/*.tiff'))
        tiff_filepath = f'{tmp_folder}/{num_tiff_files - 1}-3857.tiff'
        create_tiles(tmp_folder, aggregation_tile, tiff_filepath, buffer_pixels)
        create_archive(tmp_folder, out_filepath, aggregation_tile)
    
    # remote_aggregation_store = f'{local_config.remote_aggregation_store_path}/'
    # local_aggregation_store = f'aggregation-store/'
    # utils.rsync(...)

# filepaths = sorted(glob(f'aggregation-store/{aggregation_id}/*.csv'))
# if local_config.from_filepath is not None and local_config.to_filepath is not None:
#     filepaths = filepaths[local_config.from_filepath:local_config.to_filepath]
# main(filepaths)
