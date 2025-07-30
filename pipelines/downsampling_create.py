from glob import glob
import io
from multiprocessing import Pool
import math
import shutil
import os
from datetime import datetime

import numpy as np
from PIL import Image
import cv2
import imagecodecs
import mercantile
from pmtiles.reader import Reader, MmapSource
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

import utils

def create_archive(tmp_folder, out_filepath):
    with open(out_filepath, 'wb') as f1:
        writer = Writer(f1)
        min_z = math.inf
        max_z = 0
        min_lon = math.inf
        min_lat = math.inf
        max_lon = -math.inf
        max_lat = -math.inf
        for filepath in glob(f'{tmp_folder}/*.png'):
            filename = filepath.split('/')[-1]
            z, x, y = [int(a) for a in filename.replace('.png', '').split('-')]
            
            tile_id = zxy_to_tileid(z=z, x=x, y=y)
            with open(filepath, 'rb') as f2:
                writer.write_tile(tile_id, f2.read())

            max_z = max(max_z, z)
            min_z = min(min_z, z)
            west, south, east, north = mercantile.bounds(x, y, z)
            min_lon = min(min_lon, west)
            min_lat = min(min_lat, south)
            max_lon = max(max_lon, east)
            max_lat = max(max_lat, north)

        min_lon_e7 = int(min_lon * 1e7)
        min_lat_e7 = int(min_lat * 1e7)
        max_lon_e7 = int(max_lon * 1e7)
        max_lat_e7 = int(max_lat * 1e7)

        writer.finalize(
            {
                'tile_type': TileType.PNG,
                'tile_compression': Compression.NONE,
                'min_zoom': min_z,
                'max_zoom': max_z,
                'min_lon_e7': min_lon_e7,
                'min_lat_e7': min_lat_e7,
                'max_lon_e7': max_lon_e7,
                'max_lat_e7': max_lat_e7,
                'center_zoom': int(0.5 * (min_z + max_z)),
                'center_lon_e7': int(0.5 * (min_lon_e7 + max_lon_e7)),
                'center_lat_e7': int(0.5 * (min_lat_e7 + max_lat_e7)),
            },
            {
                'attribution': '<a href="https://github.com/mapterhorn/mapterhorn">© Mapterhorn</a>'
            },
        )

def create_tile(parent_x, parent_y, parent_z, aggregation_id, tmp_folder, pmtiles_filenames):
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
            with open(f'aggregation-store/{aggregation_id}/{tile_to_pmtiles_filename[child]}' , 'r+b') as f:
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

    parent_bytes = imagecodecs.png_encode(parent_rgb)
    parent_filepath = f'{tmp_folder}/{parent_z}-{parent_x}-{parent_y}.png'
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

def main(filepaths):
    for j, filepath in enumerate(filepaths):
        print(f'downsampling {filepath}. {datetime.now()}. {j + 1} / {len(filepaths)}.')
        out_filepath = filepath.replace('-downsampling.csv', '.pmtiles')
        if os.path.isfile(out_filepath):
            print('already done...')
            continue

        _, aggregation_id, filename = filepath.split('/')
        parts = filename.split('-')
        extent_z, extent_x, extent_y, parent_zoom = [int(a) for a in parts[:4]]
        extent = mercantile.Tile(x=extent_x, y=extent_y, z=extent_z)
        tmp_folder = filepath.replace('-downsampling.csv', '-tmp')
        utils.create_folder(tmp_folder)

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
        
        argument_tuples = []
        for parent in parents:
            argument_tuples.append((parent.x, parent.y, parent.z, aggregation_id, tmp_folder, pmtiles_filenames))

        with Pool() as pool:
            pool.starmap(create_tile, argument_tuples)
        
        create_archive(tmp_folder, out_filepath)

        shutil.rmtree(tmp_folder)

if __name__ == '__main__':
    child_zoom_to_filepaths = {}
    for filepath in sorted(glob(f'aggregation-store/01K0YBQDR8DD963PF74AJXGHZ3/*-downsampling.csv')):
        filename = filepath.split('/')[-1]
        extent_z, extent_x, extent_y, child_zoom = [int(a) for a in filename.replace('-downsampling.csv', '').split('-')]
        tile = mercantile.Tile(x=extent_x, y=extent_y, z=extent_z)
        filter_tile = mercantile.Tile(x=4, y=2, z=3)
        if child_zoom > 11:
            continue
        if extent_z < filter_tile.z:
            continue
        elif extent_z == filter_tile.z:
            if tile != filter_tile:
                continue
        elif mercantile.parent(tile, zoom=filter_tile.z) != filter_tile:
            continue
        if child_zoom not in child_zoom_to_filepaths:
            child_zoom_to_filepaths[child_zoom] = []
        child_zoom_to_filepaths[child_zoom].append(filepath)

    child_zooms = list(reversed(sorted(list(child_zoom_to_filepaths.keys()))))
    for child_zoom in child_zooms:
        main(child_zoom_to_filepaths[child_zoom])
