import subprocess
from pathlib import Path
from glob import glob
import json
from datetime import datetime
import time
import shutil

import mercantile

def run_command(command):
    print(command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    err = stderr.decode()
    if err != '':
        print(err)
    out = stdout.decode()
    if out != '':
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
