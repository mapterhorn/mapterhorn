from glob import glob
import math
from multiprocessing import Pool
import os
import json

import mercantile
import rasterio

import utils

def create_tiles(tmp_folder, aggregation_tile, tiff_filepath, buffer_pixels):
    base_x = aggregation_tile.x
    base_y = aggregation_tile.y
    base_z = aggregation_tile.z

    child_z = None
    with rasterio.open(tiff_filepath) as src:
        assert len(src.block_shapes) >= 1
        assert src.block_shapes[0] == (512, 512)
        horizontal_block_count = (src.width - 2 * buffer_pixels) / 512
        assert math.floor(horizontal_block_count) == horizontal_block_count
        child_z = base_z + int(math.log2(horizontal_block_count))
    argument_tuples = []
    z = child_z
    x_min = base_x * 2 ** (z - base_z)
    y_min = base_y * 2 ** (z - base_z)
    for i, x in enumerate(range(x_min, x_min + 2 ** (z - base_z))):
        for j, y in enumerate(range(y_min, y_min + 2 ** (z - base_z))):
            out_filepath = f'{tmp_folder}/{z}-{x}-{y}.webp'
            argument_tuples.append((i, j, tiff_filepath, out_filepath, buffer_pixels))
    
    with Pool() as pool:
        pool.starmap(create_tile, argument_tuples)

def create_tile(i, j, tiff_filepath, out_filepath, buffer_pixels):
    col_start = i * 512 + buffer_pixels
    col_end = (i + 1) * 512 + buffer_pixels
    row_start = j * 512 + buffer_pixels
    row_end = (j + 1) * 512 + buffer_pixels
    window = rasterio.windows.Window(
        col_off=col_start,
        row_off=row_start,
        width=col_end - col_start,
        height=row_end - row_start
    )
    subdata = None
    with rasterio.open(tiff_filepath) as src: 
        subdata = src.read(1, window=window, out_shape=(512, 512))
    subdata[subdata == -9999] = 0
    utils.save_terrarium_tile(subdata, out_filepath)

def main(filepaths):
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    for j, filepath in enumerate(filepaths):
        print(f'tiling {filepath}. {j + 1} / {len(filepaths)}.')
        filename = filepath.split('/')[-1]

        z, x, y, child_z = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]

        tmp_folder = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-tmp'


        pmtiles_done_filepath = f'{tmp_folder}/pmtiles-done'
        if os.path.isfile(pmtiles_done_filepath):
            print(f'tiling {filename} already done...')
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

        aggregation_tile = mercantile.Tile(x=x, y=y, z=z)
        out_folder = utils.get_pmtiles_folder(x, y, z)
        utils.create_folder(out_folder)
        out_filepath = f'{out_folder}/{z}-{x}-{y}-{child_z}.pmtiles'
        create_tiles(tmp_folder, aggregation_tile, tiff_filepath, buffer_pixels)
        utils.create_archive(tmp_folder, out_filepath)
        utils.run_command(f'touch {pmtiles_done_filepath}')
