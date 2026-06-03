from glob import glob
import math
import time
import sys
import json
import os

import mercantile
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.reader import Reader, MmapSource, all_tiles
from pmtiles.writer import Writer

import utils

def get_parent_to_filepaths(only_dirty, num_aggregations):
    filepaths = sorted(glob('pmtiles-store/*.pmtiles') + glob('pmtiles-store/*/*.pmtiles'))

    parent_to_filepath = {}
    dirty_parents = get_dirty_parents(num_aggregations)

    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        z, x, y, child_z = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
        
        parent = None
        if child_z <= 12:
            parent = mercantile.Tile(x=0, y=0, z=0)
        else:
            assert z >= 6
            if z == 6:
                parent = mercantile.Tile(x=x, y=y, z=z)
            else:
                parent = mercantile.parent(mercantile.Tile(x=x, y=y, z=z), zoom=6)
        
        if only_dirty and parent not in dirty_parents:
            continue

        if parent not in parent_to_filepath:
            parent_to_filepath[parent] = []

        parent_to_filepath[parent].append(filepath)

    return parent_to_filepath

def get_dirty_parents(num_aggregations):
    dirty_parents = set([mercantile.Tile(x=0, y=0, z=0)])

    aggregation_ids = utils.get_aggregation_ids()
    assert len(aggregation_ids) >= num_aggregations

    for offset in range(num_aggregations):
        current_aggregation_id = aggregation_ids[-1 - offset]
        last_aggregation_id = None if len(aggregation_ids) == 1 else aggregation_ids[-2 - offset]
        aggregation_filenames = utils.get_dirty_aggregation_filenames(current_aggregation_id, last_aggregation_id)
        
        for filename in aggregation_filenames:
            z, x, y, child_z = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]
            if child_z >= 13:
                dirty_parents.add(mercantile.parent(mercantile.Tile(x=x, y=y, z=z), zoom=6))

    return list(dirty_parents)

def read_full_archive(filepath):
    tile_id_to_bytes = {}
    with open(filepath , 'r+b') as f2:
        reader = Reader(MmapSource(f2))
        for tile_tuple, tile_bytes in all_tiles(reader.get_bytes):
            tile_id = zxy_to_tileid(*tile_tuple)
            tile_id_to_bytes[tile_id] = tile_bytes
    return tile_id_to_bytes

def create_archive(filepaths, name):
    utils.create_folder('bundle-store')
    out_filepath = f'bundle-store/{name}.pmtiles'
    checksum = None
    min_z = math.inf
    max_z = 0
    min_lon = math.inf
    min_lat = math.inf
    max_lon = -math.inf
    max_lat = -math.inf
    with open(out_filepath, 'wb') as f1:
        hash_writer = utils.HashWriter(f1)
        writer = Writer(hash_writer)
        
        tile_ids_and_filepaths = []

        j = 0
        for filepath in filepaths:
            filename = filepath.split('/')[-1]
            z, x, y, child_z = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
            parent = mercantile.Tile(x=x, y=y, z=z)
            tiles = []
            if z == child_z:
                tiles.append(parent)
            else:
                tiles += mercantile.children(parent, zoom=child_z)
            for tile in tiles:
                tile_id = zxy_to_tileid(tile.z, tile.x, tile.y)
                tile_ids_and_filepaths.append((tile_id, filepath))
        
            max_z = max(max_z, child_z)
            min_z = min(min_z, child_z)
            west, south, east, north = mercantile.bounds(x, y, z)
            min_lon = min(min_lon, west)
            min_lat = min(min_lat, south)
            max_lon = max(max_lon, east)
            max_lat = max(max_lat, north)
            j += 1
            if j % 1000 == 0:
                print(f'prepared {j:_} / {len(filepaths):_} filepaths...')

        tile_ids_and_filepaths = sorted(tile_ids_and_filepaths)
        
        last_filepath = None
        tile_id_to_bytes = None

        j = 0
        start = time.time()
        for tile_id, filepath in tile_ids_and_filepaths:
            if filepath != last_filepath:
                last_filepath = filepath
                tile_id_to_bytes = read_full_archive(filepath)
            writer.write_tile(tile_id, tile_id_to_bytes[tile_id])

            j += 1
            if j % 10_000 == 0:
                tic = time.time()
                time_so_far = tic - start
                expected_duration = time_so_far * len(tile_ids_and_filepaths) / j
                finishes_in = expected_duration - time_so_far
                print(f'Processed {j:_} / {len(tile_ids_and_filepaths):_} tiles in {int(time_so_far / 60)} min {int(time_so_far) % 60} s. Finishes in {int(finishes_in / 3600)} h {int(finishes_in / 60) % 60} min...')

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
                'attribution': '<a href="https://mapterhorn.com/attribution">© Mapterhorn</a>',
            },
        )
        checksum = hash_writer.md5.hexdigest()

    utils.create_folder('meta-store/bundle')
    filesize = os.path.getsize(out_filepath)
    with open(f'meta-store/bundle/{name}.json', 'w') as f:
        json.dump({
            'size': filesize,
            'md5sum': checksum,
            'min_lon': min_lon,
            'min_lat': min_lat,
            'max_lon': max_lon,
            'max_lat': max_lat,
            'min_zoom': min_z,
            'max_zoom': max_z,
        }, f, indent=2)

def get_name_from_parent(parent):
    name = None
    if parent == mercantile.Tile(x=0, y=0, z=0):
        name = 'planet'
    else:
        name = f'{parent.z}-{parent.x}-{parent.y}'
    return name

def main():
    num_aggregations = None
    if len(sys.argv) == 2:
        num_aggregations = int(sys.argv[1])
        print(f'bundling the last {num_aggregations} aggregation(s)...')
    else:
        print('Not enough arguments. Usage: bundle.py {{num_aggregations}}')
        exit()
    
    dirty_only = True
    parent_to_filepaths = get_parent_to_filepaths(dirty_only, num_aggregations)
    for parent in parent_to_filepaths:
        name = get_name_from_parent(parent)
        print(name)
        create_archive(parent_to_filepaths[parent], name)

    print(f'The following {len(parent_to_filepaths.keys())} file(s) were created:')
    for parent in parent_to_filepaths.keys():
        print(f'{get_name_from_parent(parent)}.pmtiles')

if __name__ == '__main__':
    main()
