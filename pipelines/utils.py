import subprocess
from pathlib import Path
from glob import glob
import json
from datetime import datetime
import math
import os

import numpy as np

import mercantile
import imagecodecs
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

def run_command(command, silent=True):
    if not silent:
        print(command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    err = stderr.decode()
    if err != '' and not silent:
        print(err)
    out = stdout.decode()
    if out != '' and not silent:
        print(out)

def create_folder(path):
    folder_path = Path(path)
    folder_path.mkdir(parents=True, exist_ok=True)

def rsync(src, dst, skip_data_files=False):
    command = f'rsync -avh {src} {dst}'
    if skip_data_files:
        command += ' --exclude "*.tiff"'
        command += ' --exclude "*.tif"'
        command += ' --exclude "*.pmtiles"'
    run_command(command)

def get_collection_ids(source):
    '''
    returns collection ids ordered from oldest to newest
    '''
    paths = glob(f'cogify-store/3857/{source}/*')
    collection_ids = [path.split('/')[-1] for path in paths]
    timestamps = []
    for collection_id in collection_ids:
        with open(f'cogify-store/3857/{source}/{collection_id}/collection.json') as f:
            collection = json.load(f)
            time_string = collection['extent']['temporal']['interval'][0][0]
            time_string = time_string.replace('Z', '+00:00')
            timestamps.append(datetime.fromisoformat(time_string))

    return [sorted_id for _, sorted_id in sorted(zip(timestamps, collection_ids))]

def get_collection_items(source, collection_id):
    paths = glob(f'cogify-store/3857/{source}/{collection_id}/*.json')
    filenames = [path.split('/')[-1] for path in paths]
    return [item for item in filenames if item not in ['collection.json', 'covering.json', 'source.json']]

def get_aggregation_ids():
    '''
    returns aggregation ids ordered from oldest to newest
    '''
    return list(sorted([path.split('/')[-1] for path in glob(f'aggregation-store/*')]))

def get_maxzoom(source, collection_id):
    '''
    maxzoom is for 512 pixel tiles such that resolution is roughly 40'000km / 2**maxzoom / 512
    '''
    with open(f'cogify-store/3857/{source}/{collection_id}/covering.geojson') as f:
        covering = json.load(f)
        # the levels in the json are probably for 256er tiles. we use 512er tiles, so 1 less.
        return covering['features'][0]['properties']['linz_basemaps:options']['zoomLevel'] - 1

def are_tiles_overlapping(tile, other_tile):
    a = None
    b = None
    if tile.z > other_tile.z:
        a = mercantile.parent(tile, zoom=other_tile.z)
        b = other_tile
    elif tile.z == other_tile.z:
        a = tile
        b = other_tile
    else:
        a = tile
        b = mercantile.parent(other_tile, zoom=tile.z)
    return a == b

def save_terrarium_tile(data, filepath):
    data += 32768
    rgb = np.zeros((512, 512, 3), dtype=np.uint8)
    rgb[..., 0] = data // 256
    rgb[..., 1] = data % 256
    rgb[..., 2] = (data - np.floor(data)) * 256
    with open(filepath, 'wb') as f:
        f.write(imagecodecs.png_encode(rgb))

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

def get_aggregation_item_string(aggregation_id, filename):
    filepath = f'aggregation-store/{aggregation_id}/{filename}'
    if not os.path.isfile(filepath):
        return None
    
    with open(filepath) as f:
        return ''.join(f.readlines())

def get_dirty_aggregation_filenames(current_aggregation_id, last_aggregation_id):
    filepaths = sorted(glob(f'aggregation-store/{current_aggregation_id}/*-aggregation.csv'))

    dirty_filenames = []
    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        current = get_aggregation_item_string(current_aggregation_id, filename)
        last = get_aggregation_item_string(last_aggregation_id, filename)
        if current != last:
            dirty_filenames.append(filename)
    return dirty_filenames