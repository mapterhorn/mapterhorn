import rasterio
import math
import numpy as np
from PIL import Image
import io

from pmtiles.tile import zxy_to_tileid, tileid_to_zxy, TileType, Compression
from pmtiles.writer import Writer

import utils

base_z = 14
base_x = 8520
base_y = 5836

def save_png(subdata, filename):
    subdata += 32768
    rows, cols = 512, 512
    rgb = np.zeros((3, rows, cols), dtype=np.uint8)
    rgb[0] = subdata // 256
    rgb[1] = np.floor(subdata % 256)
    rgb[2] = np.floor((subdata - np.floor(subdata)) * 256)
    with rasterio.open(
        filename,
        'w',
        driver='PNG',
        height=rows,
        width=cols,
        count=3,
        dtype='uint8',
    ) as dst:
        dst.write(rgb[0], 1)
        dst.write(rgb[1], 2)
        dst.write(rgb[2], 3)

def save_webp(subdata, filename):
    subdata += 32768
    rows, cols = 512, 512
    rgb = np.zeros((rows, cols, 3), dtype=np.uint8)
    rgb[..., 0] = subdata // 256
    rgb[..., 1] = np.floor(subdata % 256)
    rgb[..., 2] = np.floor((subdata - np.floor(subdata)) * 256)
    image = Image.fromarray(rgb, mode='RGB')
    buffer = io.BytesIO()
    image.save(buffer, format='WEBP', lossless=True)
    image_bytes = buffer.getvalue()
    # image.save(filename, format='WEBP', lossless=True)

with rasterio.open(f'../backup-scripts/{base_z}-{base_x}-{base_y}-lzw.tiff') as src:
    print('block_shapes', src.block_shapes)
    # [(512, 512)]
    assert len(src.block_shapes) == 1
    assert src.block_shapes[0] == (512, 512)
    horizontal_block_count = src.width / 512
    vertical_block_count = src.height / 512
    assert math.floor(horizontal_block_count) == horizontal_block_count
    assert horizontal_block_count == vertical_block_count
    max_z = base_z + int(math.log2(horizontal_block_count))
    print(max_z)
    print('width', src.width)
    # 8192
    print('height', src.height)
    # 8192
    print('overviews', src.overviews(1))
    assert tuple(src.overviews(1)) == (2, 4, 8, 16)

    filename = f'pmtiles-store/out.pmtiles'
    with open(filename, "wb") as f:
        writer = Writer(f)
        for z in [14, 15, 16]:
            print(f'z={z}')
            x_min = base_x * 2 ** (z - base_z)
            y_min = base_y * 2 ** (z - base_z)
            factor = 2 ** (max_z - z)
            data = src.read(1, out_shape=(1, src.width // factor, src.height // factor))
            for i, x in enumerate(range(x_min, x_min + 2 ** (z - base_z))):
                for j, y in enumerate(range(y_min, y_min + 2 ** (z - base_z))):
                    subdata = data[(j * 512):(j * 512 + 512), (i * 512):(i * 512 + 512)]
                    if z < 18 or -9999 not in subdata:
                        subdata += 32768
                        rows, cols = 512, 512
                        rgb = np.zeros((rows, cols, 3), dtype=np.uint8)
                        rgb[..., 0] = subdata // 256
                        rgb[..., 1] = np.floor(subdata % 256)
                        rgb[..., 2] = np.floor((subdata - np.floor(subdata)) * 256)
                        image = Image.fromarray(rgb, mode='RGB')
                        buffer = io.BytesIO()
                        image.save(buffer, format='WEBP', lossless=True)
                        image_bytes = buffer.getvalue()

                        tile_id = zxy_to_tileid(z, x, y)
                        writer.write_tile(tile_id, image_bytes)

                        # filename = f'pmtiles-store/{z}-{x}-{y}.png'
                        # save_png(np.array(subdata), filename)
                        # filename = f'pmtiles-store/{z}-{x}-{y}.webp'
                        # save_webp(np.array(subdata), filename)
                        # subdata += 32768
                        # rows, cols = 512, 512
                        # rgb = np.zeros((3, rows, cols), dtype=np.uint8)
                        # rgb[0] = subdata // 256
                        # rgb[1] = np.floor(subdata % 256)
                        # rgb[2] = np.floor((subdata - np.floor(subdata)) * 256)
                        # with rasterio.open(
                        #     filename,
                        #     'w',
                        #     driver='PNG',
                        #     height=rows,
                        #     width=cols,
                        #     count=3,
                        #     dtype='uint8',
                        # ) as dst:
                        #     dst.write(rgb[0], 1)
                        #     dst.write(rgb[1], 2)
                        #     dst.write(rgb[2], 3)
        writer.finalize(
            {
                "tile_type": TileType.WEBP,
                "tile_compression": Compression.NONE,
                "min_zoom": 14,
                "max_zoom": 16,
                "min_lon_e7": int(-180.0 * 10000000),
                "min_lat_e7": int(-85.0 * 10000000),
                "max_lon_e7": int(180.0 * 10000000),
                "max_lat_e7": int(85.0 * 10000000),
                "center_zoom": 0,
                "center_lon_e7": 0,
                "center_lat_e7": 0,
            },
            {
                "attribution": '<a href="https://github.com/tilezen/joerd/blob/master/docs/attribution.md">Tilezen Joerd: Attribution</a>'
            },
        )


    