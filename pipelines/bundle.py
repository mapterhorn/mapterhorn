from glob import glob
import math
import os
import json
import tempfile
import gzip
import shutil

import mercantile
from pmtiles.tile import zxy_to_tileid, tileid_to_zxy, TileType, Compression, Entry, serialize_directory, serialize_header
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

def create_archive(filepaths, out_filepath):
    with open(out_filepath, 'wb') as f1:
        writer = Writer(f1)
        min_z = math.inf
        max_z = 0
        min_lon = math.inf
        min_lat = math.inf
        max_lon = -math.inf
        max_lat = -math.inf
        for j, filepath in enumerate(filepaths):
            filename = filepath.split('/')[-1]
            print(f'{filename} {j + 1} / {len(filepaths)}')
            z, x, y, _ = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
            
            with open(filepath , 'r+b') as f2:
                reader = Reader(MmapSource(f2))
                for tile_tuple, tile_bytes in all_tiles(reader.get_bytes):
                    tile_id = zxy_to_tileid(*tile_tuple)
                    writer.write_tile(tile_id, tile_bytes)

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

def get_md5sum(filepath):
    out, _ = utils.run_command(f'md5sum {filepath}')
    return out.strip().split('  ')[0]    

def main():
    parent_to_filepaths = get_parent_to_filepaths()
    utils.create_folder('bundle-store')
    lines = ['filename,md5sum,size_gigabytes\n']
    for parent in parent_to_filepaths:
        filename = None
        if parent == mercantile.Tile(x=0, y=0, z=0):
            filename = 'planet.pmtiles'
        else:
            filename = f'{parent.z}-{parent.x}-{parent.y}.pmtiles'
        out_filepath = f'bundle-store/{filename}'
        print(filename)
        create_archive(parent_to_filepaths[parent], out_filepath)
        # md5sum = get_md5sum(out_filepath)
        # size = os.path.getsize(out_filepath)
        # lines.append(f'{filename},{md5sum[:8]},{int(size/1024**3 * 100)/100}\n')

    with open('bundle-store/index.csv', 'w') as f:
        f.writelines(lines)

if __name__ == '__main__':
    main()
