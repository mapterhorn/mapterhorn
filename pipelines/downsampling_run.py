from glob import glob
import io
from multiprocessing import Pool
import shutil
from datetime import datetime
import os

import numpy as np
from PIL import Image
import imagecodecs
import mercantile
from pmtiles.reader import Reader, MmapSource

import utils

def create_tile(parent_x, parent_y, parent_z, tmp_folder, pmtiles_filenames):
    tile_to_pmtiles_filename = get_tile_to_pmtiles_filename(pmtiles_filenames)
    full_data = np.zeros((1024, 1024), dtype=np.float32)
    for row_offset in range(2):
        for col_offset in range(2):
            child_x = 2 * parent_x + col_offset
            child_y = 2 * parent_y + row_offset
            child_z = parent_z + 1
            child = mercantile.Tile(x=child_x, y=child_y, z=child_z)
            if child not in tile_to_pmtiles_filename:
                continue
            child_bytes = None
            filename = tile_to_pmtiles_filename[child]
            file_z, file_x, file_y, _ = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
            pmtiles_folder = utils.get_pmtiles_folder(file_x, file_y, file_z)
            with open(f'{pmtiles_folder}/{filename}' , 'r+b') as f:
                reader = Reader(MmapSource(f))
                child_bytes = reader.get(child_z, child_x, child_y)
            child_rgb = np.array(Image.open(io.BytesIO(child_bytes)), dtype=np.float32)
            row_start = 512 * row_offset
            row_end = 512 * (row_offset + 1)
            col_start = 512 * col_offset
            col_end = 512 * (col_offset + 1)
            # (red * 256 + green + blue / 256) - 32768
            full_data[row_start:row_end, col_start:col_end] = child_rgb[:, :, 0] * 256.0 + child_rgb[:, :, 1] + child_rgb[:, :, 2] / 256.0 - 32768.0
            
    parent_data = full_data.reshape((512, 2, 512, 2)).mean(axis=(1, 3)) # downsample by 4x4 pixel averaging

    parent_data += 32768.0
    parent_rgb = np.zeros((512, 512, 3), dtype=np.uint8)
    parent_rgb[:, :, 0] = parent_data // 256
    parent_rgb[:, :, 1] = np.floor(parent_data % 256)
    parent_rgb[:, :, 2] = np.floor((parent_data - np.floor(parent_data)) * 256)

    parent_bytes = imagecodecs.webp_encode(parent_rgb, lossless=True)
    parent_filepath = f'{tmp_folder}/{parent_z}-{parent_x}-{parent_y}.webp'
    with open(parent_filepath, 'wb') as f:
        f.write(parent_bytes)

def get_tile_to_pmtiles_filename(pmtiles_filenames):
    tile_to_pmtiles_filename = {}
    for pmtiles_filename in pmtiles_filenames:
        pmtiles_z, pmtiles_x, pmtiles_y, child_zoom = [int(a) for a in pmtiles_filename.replace('.pmtiles', '').split('-')]
        children = None
        if pmtiles_z == child_zoom:
            children = [mercantile.Tile(x=pmtiles_x, y=pmtiles_y, z=pmtiles_z)]
        else:
            children = list(mercantile.children(mercantile.Tile(x=pmtiles_x, y=pmtiles_y, z=pmtiles_z), zoom=child_zoom))
        for child in children:
            tile_to_pmtiles_filename[child] = pmtiles_filename
    return tile_to_pmtiles_filename

def downsample_single(filepath):
    _, __, filename = filepath.split('/')
    print(f'downsampling {filename}. {datetime.now()}')
    if os.path.isfile(f'{filepath}.done'):
        print('already done...')
        return
    parts = filename.split('-')
    extent_z, extent_x, extent_y, parent_zoom = [int(a) for a in parts[:4]]

    out_folder = utils.get_pmtiles_folder(extent_x, extent_y, extent_z)
    utils.create_folder(out_folder)
    out_filepath = f'{out_folder}/{extent_z}-{extent_x}-{extent_y}-{parent_zoom}.pmtiles'

    extent = mercantile.Tile(x=extent_x, y=extent_y, z=extent_z)
    tmp_folder = f'tmp-store/{filename.replace("-downsampling.csv", "")}'
    os.makedirs(tmp_folder, exist_ok=True)

    pmtiles_filenames = None
    with open(filepath) as f:
        pmtiles_filenames = f.readlines()
        pmtiles_filenames = pmtiles_filenames[1:] # skip header
        pmtiles_filenames = [a.strip() for a in pmtiles_filenames]

    parents = None
    if extent_z == parent_zoom:
        parents = [extent]
    else:
        parents = list(mercantile.children(extent, zoom=parent_zoom))
    
    for parent in parents:
        create_tile(parent.x, parent.y, parent.z, tmp_folder, pmtiles_filenames)
    
    utils.create_archive(tmp_folder, out_filepath)

    shutil.rmtree(tmp_folder)
    os.rename(f'{filepath}.todo', f'{filepath}.done')
    print(f'{filepath} done.')

def downsample_multiple(filepaths):
    argument_tuples = [(filepath,) for filepath in filepaths]
    with Pool() as pool:
        pool.starmap(downsample_single, argument_tuples, chunksize=1)

def get_child_zoom_to_filepaths():
    child_zoom_to_filepaths = {}
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]
    for todo_filepath in sorted(glob(f'aggregation-store/{aggregation_id}/*-downsampling.csv.todo')):
        filename = todo_filepath.split('/')[-1]
        _, __, ___, child_zoom = [int(a) for a in filename.replace('-downsampling.csv.todo', '').split('-')]
        if child_zoom not in child_zoom_to_filepaths:
            child_zoom_to_filepaths[child_zoom] = []
        child_zoom_to_filepaths[child_zoom].append(todo_filepath.replace('.todo', ''))
    return child_zoom_to_filepaths

def main():
    child_zoom_to_filepaths = get_child_zoom_to_filepaths()
    child_zooms = list(reversed(sorted(list(child_zoom_to_filepaths.keys()))))
    for child_zoom in child_zooms:
        print(child_zoom)
        print(len(child_zoom_to_filepaths[child_zoom]))
        downsample_multiple(child_zoom_to_filepaths[child_zoom])

if __name__ == '__main__':
    main()