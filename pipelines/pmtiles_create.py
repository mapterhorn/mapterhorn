import math
import time
import io
from glob import glob
import shutil

import numpy as np
import rasterio
from PIL import Image
import mercantile

from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

import utils

def get_tmp_folder(base_x, base_y, base_z):
    return f'pmtiles-store/{base_z}-{base_x}-{base_y}-tmp'

def create_tiles(base_x, base_y, _base_z):
    with rasterio.env.Env(GDAL_CACHEMAX=256):
        in_filename = f'aggregation-store/01K045PY15X4X8FMA4FSKD7V1Y/{base_z}-{base_x}-{base_y}.tiff'
        print(f'converting {in_filename} to pmtiles...')
        max_z = None
        with rasterio.open(in_filename) as src:
            assert len(src.block_shapes) == 1
            assert src.block_shapes[0] == (512, 512)
            horizontal_block_count = src.width / 512
            vertical_block_count = src.height / 512
            assert math.floor(horizontal_block_count) == horizontal_block_count
            assert horizontal_block_count == vertical_block_count
            max_z = base_z + int(math.log2(horizontal_block_count))
            assert len(src.overviews(1)) == max_z - base_z

        tmp_folder = get_tmp_folder(base_x, base_y, base_z)
        utils.create_folder(tmp_folder)

        for z in range(base_z, max_z + 1):
            print(f'z={z}')
            tic = time.time()
            subtime = 0
            x_min = base_x * 2 ** (z - base_z)
            y_min = base_y * 2 ** (z - base_z)
            factor = 2 ** (max_z - z)
            
            for i, x in enumerate(range(x_min, x_min + 2 ** (z - base_z))):
                for j, y in enumerate(range(y_min, y_min + 2 ** (z - base_z))):
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
                    with rasterio.open(in_filename) as src: 
                        subdata = src.read(1, window=window, out_shape=(512, 512))
                    subdata[subdata == -9999] = 0
                    subdata += 32768
                    rows, cols = 512, 512
                    rgb = np.zeros((rows, cols, 3), dtype=np.uint8)
                    rgb[..., 0] = subdata // 256
                    rgb[..., 1] = np.floor(subdata % 256)
                    rgb[..., 2] = np.floor((subdata - np.floor(subdata)) * 256)
                    image = Image.fromarray(rgb, mode='RGB')
                    out_filename = f'{tmp_folder}/{z}-{x}-{y}.webp'
                    image.save(out_filename, format='WEBP', lossless=True)
            print(f'z={z} finished in {time.time() - tic} s')

def create_archive(base_x, base_y, _base_z):
    out_filename = f'pmtiles-store/{base_z}-{base_x}-{base_y}.pmtiles'
    with open(out_filename, 'wb') as f1:
        writer = Writer(f1)

        tmp_folder = get_tmp_folder(base_x, base_y, base_z)
        max_z = 0
        for filepath in glob(f'{tmp_folder}/*.webp'):
            filename = filepath.split('/')[-1]
            z, x, y = [int(a) for a in filename.replace('.webp', '').split('-')]
            if z > max_z:
                max_z = z
            tile_id = zxy_to_tileid(z=z, x=x, y=y)
            with open(filepath, 'rb') as f2:
                writer.write_tile(tile_id, f2.read())

        base_tile = mercantile.Tile(base_x, base_y, base_z)
        bounds = mercantile.bounds(base_tile)
        min_lon_e7 = int(bounds.west * 1e7)
        min_lat_e7 = int(bounds.south * 1e7)
        max_lon_e7 = int(bounds.east * 1e7)
        max_lat_e7 = int(bounds.north * 1e7)

        writer.finalize(
            {
                'tile_type': TileType.WEBP,
                'tile_compression': Compression.NONE,
                'min_zoom': base_z,
                'max_zoom': max_z,
                'min_lon_e7': min_lon_e7,
                'min_lat_e7': min_lat_e7,
                'max_lon_e7': max_lon_e7,
                'max_lat_e7': max_lat_e7,
                'center_zoom': int(0.5 * (base_z + max_z)),
                'center_lon_e7': int(0.5 * (min_lon_e7 + max_lon_e7)),
                'center_lat_e7': int(0.5 * (min_lat_e7 + max_lat_e7)),
            },
            {
                'attribution': '<a href="https://github.com/mapterhorn/mapterhorn">© Mapterhorn</a>'
            },
        )


base_z = 12
base_x = 2138
base_y = 1431

# create_tiles(base_x, base_y, base_z)
create_archive(base_x, base_y, base_z)

# shutil.rmtree(get_tmp_folder(base_x, base_y, base_z))
