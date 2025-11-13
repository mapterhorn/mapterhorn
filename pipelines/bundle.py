from glob import glob
import math
import time
import sys

import mercantile
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.reader import Reader, MmapSource, all_tiles
from pmtiles.writer import Writer

import utils

def get_parent_to_filepaths():
    filepaths = sorted(glob('pmtiles-store/*.pmtiles') + glob('pmtiles-store/*/*.pmtiles'))

    parent_to_filepath = {}

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
        
        if parent not in parent_to_filepath:
            parent_to_filepath[parent] = []

        parent_to_filepath[parent].append(filepath)

    return parent_to_filepath

def read_full_archive(filepath):
    tile_id_to_bytes = {}
    with open(filepath , 'r+b') as f2:
        reader = Reader(MmapSource(f2))
        for tile_tuple, tile_bytes in all_tiles(reader.get_bytes):
            tile_id = zxy_to_tileid(*tile_tuple)
            tile_id_to_bytes[tile_id] = tile_bytes
    return tile_id_to_bytes

def create_archive(filepaths, out_filepath):
    with open(out_filepath, 'wb') as f1:
        writer = Writer(f1)
        min_z = math.inf
        max_z = 0
        min_lon = math.inf
        min_lat = math.inf
        max_lon = -math.inf
        max_lat = -math.inf

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

def main():
    parent_to_filepaths = get_parent_to_filepaths()
    for parent in parent_to_filepaths:
        name = None
        if parent == mercantile.Tile(x=0, y=0, z=0):
            name = 'planet'
        else:
            name = f'{parent.z}-{parent.x}-{parent.y}'
        print(name)
        folder = f'bundle-store/{name}'
        utils.create_folder(folder)
        out_filepath = f'{folder}/{name}.pmtiles'
        create_archive(parent_to_filepaths[parent], out_filepath)

if __name__ == '__main__':
    main()
