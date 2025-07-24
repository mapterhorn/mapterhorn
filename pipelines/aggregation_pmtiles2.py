from glob import glob
import math
from multiprocessing import Pool
import shutil
import os

import mercantile
import numpy as np
from PIL import Image
import rasterio
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

import local_config
import utils

def create_tiles(tmp_folder, aggregation_tile, tiff_filepath):
    base_x = aggregation_tile.x
    base_y = aggregation_tile.y
    base_z = aggregation_tile.z

    max_z = None
    with rasterio.open(tiff_filepath) as src:
        assert len(src.block_shapes) >= 1
        assert src.block_shapes[0] == (512, 512)
        horizontal_block_count = src.width / 512
        vertical_block_count = src.height / 512
        assert math.floor(horizontal_block_count) == horizontal_block_count
        assert horizontal_block_count == vertical_block_count
        max_z = base_z + int(math.log2(horizontal_block_count))
    argument_tuples = []
    for z in range(local_config.macrotile_z, max_z + 1):
        x_min = base_x * 2 ** (z - base_z)
        y_min = base_y * 2 ** (z - base_z)
        factor = 2 ** (max_z - z)
        for i, x in enumerate(range(x_min, x_min + 2 ** (z - base_z))):
            for j, y in enumerate(range(y_min, y_min + 2 ** (z - base_z))):
                out_filepath = f'{tmp_folder}/{z}-{x}-{y}.webp'
                argument_tuples.append((i, j, factor, tiff_filepath, out_filepath))
    
    with Pool() as pool:
        pool.starmap(create_tile, argument_tuples)

def create_tile(i, j, factor, tiff_filepath, out_filepath):
    col_start = i * 512 * factor
    col_end = (i + 1) * 512 * factor
    row_start = j * 512 * factor
    row_end = (j + 1) * 512 * factor
    window = rasterio.windows.Window(
        col_off=col_start,
        row_off=row_start,
        width=col_end - col_start,
        height=row_end - row_start
    )
    subdata = None
    with rasterio.env.Env(GDAL_CACHEMAX=256):
        with rasterio.open(tiff_filepath) as src: 
            subdata = src.read(1, window=window, out_shape=(512, 512))
    subdata[subdata == -9999] = 0
    subdata += 32768
    rows, cols = 512, 512
    rgb = np.zeros((rows, cols, 3), dtype=np.uint8)
    rgb[..., 0] = subdata // 256
    rgb[..., 1] = np.floor(subdata % 256)
    rgb[..., 2] = np.floor((subdata - np.floor(subdata)) * 256)
    image = Image.fromarray(rgb, mode='RGB')
    image.save(out_filepath, format='WEBP', lossless=True)

def create_archive(tmp_folder, out_filepath, aggregation_tile):
    with open(out_filepath, 'wb') as f1:
        writer = Writer(f1)
        max_z = 0
        for filepath in glob(f'{tmp_folder}/*.webp'):
            filename = filepath.split('/')[-1]
            z, x, y = [int(a) for a in filename.replace('.webp', '').split('-')]
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
                'tile_type': TileType.WEBP,
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

def main():
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    filepaths = sorted(glob(f'aggregation-store/{aggregation_id}/*.csv'))
    for j, filepath in enumerate(filepaths):
        print(f'working on {filepath}. {j + 1} / {len(filepaths)}.')
        filename = filepath.split('/')[-1]

        tmp_folder = f'aggregation-store/{aggregation_id}/{filename.replace(".csv", "")}-tmp'
        # if os.path.isfile(f'{tmp_folder}/merged.tiff'):
        #     print('merged.tiff file exists already...')
        #     continue

        z, x, y = [int(a) for a in filename.replace('.csv', '').split('-')]

        # if z != 6 or x != 34 or y != 22:
        #     continue
        
        aggregation_tile = mercantile.Tile(x=x, y=y, z=z)
        tiff_filepath = f'{tmp_folder}/merged.tiff'
        create_tiles(tmp_folder, aggregation_tile, tiff_filepath)
        out_filepath = f'aggregation-store/{aggregation_id}/{filename.replace(".csv", ".pmtiles")}'
        create_archive(tmp_folder, out_filepath, aggregation_tile)

        # shutil.rmtree(tmp_folder)
    
    # remote_aggregation_store = f'{local_config.remote_aggregation_store_path}/'
    # local_aggregation_store = f'aggregation-store/'
    # utils.rsync(...)

main()
