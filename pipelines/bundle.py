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

import utils

#### START COPY https://github.com/protomaps/PMTiles/blob/main/python/pmtiles/pmtiles/writer.py

def build_roots_leaves(entries, leaf_size):
    root_entries = []
    leaves_bytes = b""
    num_leaves = 0

    i = 0
    while i < len(entries):
        num_leaves += 1
        serialized = serialize_directory(entries[i : i + leaf_size])
        root_entries.append(
            Entry(entries[i].tile_id, len(leaves_bytes), len(serialized), 0)
        )
        leaves_bytes += serialized
        i += leaf_size

    return serialize_directory(root_entries), leaves_bytes, num_leaves


def optimize_directories(entries, target_root_len):
    test_bytes = serialize_directory(entries)
    if len(test_bytes) < target_root_len:
        return test_bytes, b"", 0

    leaf_size = 4096
    while True:
        root_bytes, leaves_bytes, num_leaves = build_roots_leaves(entries, leaf_size)
        if len(root_bytes) < target_root_len:
            return root_bytes, leaves_bytes, num_leaves
        leaf_size *= 2


class Writer:
    def __init__(self, f, tmp_dir):
        self.f = f
        self.tile_entries = []
        self.hash_to_offset = {}
        self.tile_f = tempfile.TemporaryFile(dir=tmp_dir)
        self.offset = 0
        self.addressed_tiles = 0
        self.clustered = True

    def write_tile(self, tileid, data):
        if len(self.tile_entries) > 0 and tileid < self.tile_entries[-1].tile_id:
            self.clustered = False

        hsh = hash(data)
        if hsh in self.hash_to_offset:
            last = self.tile_entries[-1]
            found = self.hash_to_offset[hsh]
            if tileid == last.tile_id + last.run_length and last.offset == found:
                self.tile_entries[-1].run_length += 1
            else:
                self.tile_entries.append(Entry(tileid, found, len(data), 1))
        else:
            self.tile_f.write(data)
            self.tile_entries.append(Entry(tileid, self.offset, len(data), 1))
            self.hash_to_offset[hsh] = self.offset
            self.offset += len(data)

        self.addressed_tiles += 1

    def finalize(self, header, metadata):
        header["addressed_tiles_count"] = self.addressed_tiles
        header["tile_entries_count"] = len(self.tile_entries)
        header["tile_contents_count"] = len(self.hash_to_offset)

        self.tile_entries = sorted(self.tile_entries, key=lambda e: e.tile_id)

        header["min_zoom"] = tileid_to_zxy(self.tile_entries[0].tile_id)[0]
        header["max_zoom"] = tileid_to_zxy(self.tile_entries[-1].tile_id)[0]

        root_bytes, leaves_bytes, num_leaves = optimize_directories(
            self.tile_entries, 16384 - 127
        )

        compressed_metadata = gzip.compress(json.dumps(metadata).encode())
        header["clustered"] = self.clustered
        header["internal_compression"] = Compression.GZIP
        header["root_offset"] = 127
        header["root_length"] = len(root_bytes)
        header["metadata_offset"] = header["root_offset"] + header["root_length"]
        header["metadata_length"] = len(compressed_metadata)
        header["leaf_directory_offset"] = (
            header["metadata_offset"] + header["metadata_length"]
        )
        header["leaf_directory_length"] = len(leaves_bytes)
        header["tile_data_offset"] = (
            header["leaf_directory_offset"] + header["leaf_directory_length"]
        )
        header["tile_data_length"] = self.offset

        header_bytes = serialize_header(header)

        self.f.write(header_bytes)
        self.f.write(root_bytes)
        self.f.write(compressed_metadata)
        self.f.write(leaves_bytes)
        self.tile_f.seek(0)
        shutil.copyfileobj(self.tile_f, self.f)
        self.tile_f.close()

#### END COPY https://github.com/protomaps/PMTiles/blob/main/python/pmtiles/pmtiles/writer.py

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
        writer = Writer(f1, '/data1/tmp')
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
