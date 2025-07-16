import rasterio
import math
import numpy as np

import utils

base_z = 14
base_x = 8520
base_y = 5836

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
    for z in [18, 17, 16, 15, 14]:
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
                    rgb = np.zeros((3, rows, cols), dtype=np.uint8)
                    rgb[0] = subdata // 256
                    rgb[1] = np.floor(subdata % 256)
                    rgb[2] = np.floor((subdata - np.floor(subdata)) * 256)
                    filename = f'pmtiles-store/{z}-{x}-{y}.png'

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



    