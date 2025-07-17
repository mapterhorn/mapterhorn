import math
import time
import io

import numpy as np
import rasterio
from PIL import Image
import mercantile

from pmtiles.tile import zxy_to_tileid, tileid_to_zxy, TileType, Compression
from pmtiles.writer import Writer

import utils

def convert(base_x, base_y, _base_z):
    in_filename = f'aggregation-store/01K045PY15X4X8FMA4FSKD7V1Y/{base_z}-{base_x}-{base_y}.tiff'
    print(f'converting {in_filename} to pmtiles...')
    with rasterio.open(in_filename) as src:
        assert len(src.block_shapes) == 1
        assert src.block_shapes[0] == (512, 512)
        horizontal_block_count = src.width / 512
        vertical_block_count = src.height / 512
        assert math.floor(horizontal_block_count) == horizontal_block_count
        assert horizontal_block_count == vertical_block_count
        max_z = base_z + int(math.log2(horizontal_block_count))
        assert len(src.overviews(1)) == max_z - base_z
        out_filename = f'pmtiles-store/{base_z}-{base_x}-{base_y}.pmtiles'
        with open(out_filename, 'wb') as f:
            writer = Writer(f)
            for z in range(base_z, max_z + 1):
                print(f'z={z}')
                tic = time.time()
                subtime = 0
                x_min = base_x * 2 ** (z - base_z)
                y_min = base_y * 2 ** (z - base_z)
                factor = 2 ** (max_z - z)
                data = src.read(1, out_shape=(1, src.width // factor, src.height // factor))
                
                for i, x in enumerate(range(x_min, x_min + 2 ** (z - base_z))):
                    for j, y in enumerate(range(y_min, y_min + 2 ** (z - base_z))):
                        subdata = data[(j * 512):(j * 512 + 512), (i * 512):(i * 512 + 512)]
                        subdata[subdata == -9999] = 0
                        subdata += 32768
                        rows, cols = 512, 512
                        rgb = np.zeros((rows, cols, 3), dtype=np.uint8)
                        rgb[..., 0] = subdata // 256
                        rgb[..., 1] = np.floor(subdata % 256)
                        rgb[..., 2] = np.floor((subdata - np.floor(subdata)) * 256)
                        t0 = time.time()
                        # image = Image.fromarray(rgb, mode='RGB')
                        # buffer = io.BytesIO()
                        # image.save(buffer, format='WEBP', lossless=True)
                        # image_bytes = buffer.getvalue()
                        subtime += time.time() - t0
                        tile_id = zxy_to_tileid(z, x, y)
                        # writer.write_tile(tile_id, image_bytes)
                print(f'z={z} finished in {time.time() - tic} s')
                print(f'subtime {subtime} s')

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

convert(base_x, base_y, base_z)
