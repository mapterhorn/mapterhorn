import subprocess
from pathlib import Path
from glob import glob
import math
import os

import numpy as np

import mercantile
import imagecodecs
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

macrotile_z = 12
macrotile_buffer_3857 = 250
num_overviews = 6

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

def get_collection_items(source, collection_id):
    paths = glob(f'cogify-store/3857/{source}/{collection_id}/*.json')
    filenames = [path.split('/')[-1] for path in paths]
    return [item for item in filenames if item not in ['collection.json', 'covering.json', 'source.json']]

def get_aggregation_ids():
    '''
    returns aggregation ids ordered from oldest to newest
    '''
    return list(sorted([path.split('/')[-1] for path in glob(f'aggregation-store/*')]))

def save_terrarium_tile(data, filepath):
    data += 32768
    rgb = np.zeros((512, 512, 3), dtype=np.uint8)
    rgb[..., 0] = data // 256
    rgb[..., 1] = data % 256
    rgb[..., 2] = (data - np.floor(data)) * 256
    with open(filepath, 'wb') as f:
        f.write(imagecodecs.webp_encode(rgb, lossless=True))

def create_archive(tmp_folder, out_filepath):
    with open(out_filepath, 'wb') as f1:
        writer = Writer(f1)
        min_z = math.inf
        max_z = 0
        min_lon = math.inf
        min_lat = math.inf
        max_lon = -math.inf
        max_lat = -math.inf
        for filepath in glob(f'{tmp_folder}/*.webp'):
            filename = filepath.split('/')[-1]
            z, x, y = [int(a) for a in filename.replace('.webp', '').split('-')]
            
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
                'tile_type': TileType.WEBP,
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

def get_pmtiles_folder(x, y, z):
    if z < 7:
        return f'pmtiles-store'
    if z == 7:
        return f'pmtiles-store/{z}-{x}-{y}'
    else:
        parent = mercantile.parent(mercantile.Tile(x=x, y=y, z=z), zoom=7)
        return f'pmtiles-store/{parent.z}-{parent.x}-{parent.y}'

def get_grouped_source_items(filepath):
    lines = []
    with open(filepath) as f:
        lines = f.readlines()
    lines = lines[1:] # skip header
    line_tuples = []
    for line in lines:
        source, filename, crs, maxzoom = line.strip().split(',')
        maxzoom = int(maxzoom)
        line_tuples.append((
            -maxzoom,
            source,
            crs,
            filename
        ))
    line_tuples = sorted(line_tuples)
    grouped_source_items = []

    first_line_tuple = line_tuples[0]
    last_group_signature = (first_line_tuple[0], first_line_tuple[1], first_line_tuple[2])
    current_group = [{
        'maxzoom': -first_line_tuple[0],
        'source': first_line_tuple[1],
        'crs': first_line_tuple[2],
        'filename': first_line_tuple[3],
    }]
    for line_tuple in line_tuples[1:]:
        current_group_signature = (line_tuple[0], line_tuple[1], line_tuple[2])
        if current_group_signature != last_group_signature:
            grouped_source_items.append(current_group)
            current_group = []
            last_group_signature = current_group_signature
        current_group.append({
            'maxzoom': -line_tuple[0],
            'source': line_tuple[1],
            'crs': line_tuple[2],
            'filename': line_tuple[3],
        })
    grouped_source_items.append(current_group)
    return grouped_source_items