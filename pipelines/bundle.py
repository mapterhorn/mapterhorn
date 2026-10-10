from glob import glob
import math
import time
import sys
import json
import os
from io import BytesIO

import mercantile
from pmtiles.tile import TileType, Compression, deserialize_header, deserialize_directory, Entry
from pmtiles.reader import Reader, MmapSource
from pmtiles.writer import Writer, finalize_header

import utils

def get_parent_to_filepaths(num_aggregations):
    filepaths = sorted(glob('pmtiles-store/*.pmtiles') + glob('pmtiles-store/*/*.pmtiles'))

    parent_to_filepath = {}
    dirty_parents = get_dirty_parents(num_aggregations) if num_aggregations > 0 else None

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
        
        if num_aggregations > 0 and parent not in dirty_parents:
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

def traverse_dry_run(get_bytes, header, dir_offset, dir_length):
    entries = deserialize_directory(get_bytes(dir_offset, dir_length))
    for entry in entries:
        if entry.run_length > 0:
            for i in range(entry.run_length):
                yield entry.tile_id + i, header['tile_data_offset'] + entry.offset, entry.length
        else:
            for t in traverse_dry_run(
                get_bytes,
                header,
                header['leaf_directory_offset'] + entry.offset,
                entry.length,
            ):
                yield t

def all_tiles_dry_run(get_bytes):
    header = deserialize_header(get_bytes(0, 127))
    return traverse_dry_run(get_bytes, header, header['root_offset'], header['root_length'])
    
def create_archive(filepaths, name):
    print(f'start working on {name}...')

    utils.create_folder('bundle-store')
    out_filepath = f'bundle-store/{name}.pmtiles'
    checksum = None

    min_z = math.inf
    max_z = 0
    min_lon = math.inf
    min_lat = math.inf
    max_lon = -math.inf
    max_lat = -math.inf
    tiles_filepaths = []
    tile_data_length = 0
    print('dry run over input tiles...')
    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        z, x, y, child_z = [int(a) for a in filename.replace('.pmtiles', '').split('-')]    
        max_z = max(max_z, child_z)
        min_z = min(min_z, child_z)
        west, south, east, north = mercantile.bounds(x, y, z)
        min_lon = min(min_lon, west)
        min_lat = min(min_lat, south)
        max_lon = max(max_lon, east)
        max_lat = max(max_lat, north)

        with open(filepath , 'r+b') as f:
            reader = Reader(MmapSource(f))
            for tile_id, tile_offset, tile_length in all_tiles_dry_run(reader.get_bytes):
                tiles_filepaths.append((tile_id, tile_offset, tile_length, filepath))
                tile_data_length += tile_length
    
    print(f'found {len(tiles_filepaths):_} tiles ({(tile_data_length / 1024 ** 3):.2f} GiB)')

    tiles_filepaths.sort()

    tile_entries = []
    offset = 0
    for tile_id, _, tile_length, __ in tiles_filepaths:
        tile_entries.append(Entry(tile_id, offset, tile_length, 1))
        offset += tile_length

    min_lon_e7 = int(min_lon * 1e7)
    min_lat_e7 = int(min_lat * 1e7)
    max_lon_e7 = int(max_lon * 1e7)
    max_lat_e7 = int(max_lat * 1e7)

    header = {
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
        'encoding': 'terrarium',
    }
    addressed_tiles_count  = len(tile_entries)
    tile_contents_count = len(tile_entries)
    metadata = {
        'attribution': '<a href="https://mapterhorn.com/attribution">© Mapterhorn</a>',
    }
    clustered = True
    header_bytes, root_bytes, compressed_metadata, leaves_bytes = finalize_header(header, addressed_tiles_count, tile_entries, tile_contents_count, metadata, clustered, tile_data_length)

    with open(out_filepath, 'wb') as f:

        print('writing header...')
        hash_writer = utils.HashWriter(f)
        hash_writer.write(header_bytes)
        hash_writer.write(root_bytes)
        hash_writer.write(compressed_metadata)
        hash_writer.write(leaves_bytes)

        last_filepath = None
        in_pmtiles_data = None

        print('writing tiles...')

        last_percentage = None
        bytes_written = 0
        for _, tile_offset, tile_length, filepath in tiles_filepaths:
            if last_filepath != filepath:
                last_filepath = filepath
                with open(filepath, 'rb') as f_in:
                    in_pmtiles_data = f_in.read()
            hash_writer.write(in_pmtiles_data[tile_offset:(tile_offset + tile_length)])
            bytes_written += tile_length
            percentage = int(100 * bytes_written / tile_data_length)
            if last_percentage != percentage:
                print(f'{name}.pmtiles: wrote {percentage} % ({(bytes_written / 1024 ** 3):.2f} GiB / {(tile_data_length / 1024 ** 3):.2f} GiB)')
                last_percentage = percentage

        checksum = hash_writer.md5.hexdigest()
        
    print(checksum)
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
    
    parent_to_filepaths = get_parent_to_filepaths(num_aggregations)
    for parent in parent_to_filepaths:
        name = get_name_from_parent(parent)
        print(name)
        create_archive(parent_to_filepaths[parent], name)

    print(f'The following {len(parent_to_filepaths.keys())} file(s) were created:')
    for parent in parent_to_filepaths.keys():
        print(f'{get_name_from_parent(parent)}.pmtiles')

if __name__ == '__main__':
    main()
