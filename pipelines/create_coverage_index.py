import json
from glob import glob
import os
import shutil

import mercantile
import supermercado

from pmtiles.tile import zxy_to_tileid, tileid_to_zxy, TileType, Compression
from pmtiles.writer import Writer

import utils

coverage_index_minzoom = 4

SILENT = False

def get_coverage_tiles(source):
    print(f'creating coverage csv for source {source}')

    coverage_filepath = f'polygon-store/{source}_coverage.csv'
    if os.path.isfile(coverage_filepath):
        print('already done...')
        return get_cached_coverage_tiles(coverage_filepath)

    tmp_folder = f'polygon-store/{source}_tmp'
    os.makedirs(tmp_folder, exist_ok=True)

    filepath_3857 = f'{tmp_folder}/3857.gpkg'
    if os.path.isfile(filepath_3857):
        os.remove(filepath_3857)
    
    command = f'ogr2ogr -t_srs EPSG:3857 -explodecollections {filepath_3857} polygon-store/{source}.gpkg'
    utils.run_command(command, silent=SILENT)

    filepath_buffered = f'{tmp_folder}/buffered.gpkg'
    if os.path.isfile(filepath_buffered):
        os.remove(filepath_buffered)

    command = f'ogr2ogr {filepath_buffered} {filepath_3857} -dialect SQLite -sql "SELECT ST_Union(ST_Buffer(ST_Simplify(geom, 100), 1000, 2)) AS geometry, * FROM \\"union\\""'
    utils.run_command(command, silent=SILENT)

    filepath_4326 = f'{tmp_folder}/4326.geojson'
    if os.path.isfile(filepath_4326):
        os.remove(filepath_4326)

    command = f'ogr2ogr -t_srs EPSG:4326 -explodecollections {filepath_4326} {filepath_buffered}'
    utils.run_command(command, silent=SILENT)

    data = None
    with open(filepath_4326) as f:
        data = json.load(f)

    print('burning coverage tiles...')
    tiles = [mercantile.Tile(x=tile[0], y=tile[1], z=tile[2]) for tile in supermercado.burntiles.burn(data['features'], utils.macrotile_z)]
    lines = ['x,y,z']
    for tile in tiles:
        lines.append(f'{tile.x},{tile.y},{tile.z}')

    with open(coverage_filepath, 'w') as f:
        f.write('\n'.join(lines))

    shutil.rmtree(tmp_folder)
    return tiles

def get_cached_coverage_tiles(coverage_filepath):
    lines = []
    with open(coverage_filepath) as f:
        lines = f.readlines()
    
    tiles = []
    for line in lines[1:]:
        x, y, z = [int(a) for a in line.split(',')]
        if z != utils.macrotile_z:
            print(feature)
            raise Exception('zooms do not match')

        tiles.append(mercantile.Tile(x=x, y=y, z=z))
    
    return tiles

def create_coverage_pmtiles():
    tile_to_sources = {}
    source_to_metadata = {}
    polygon_filepaths = reversed(sorted(glob('polygon-store/*.gpkg')))
    for polygon_filepath in polygon_filepaths:
        source = polygon_filepath.replace('polygon-store/', '').replace('.gpkg', '')
        print(f'reading {source}...')
        tiles = get_coverage_tiles(source)
        print(f'found {len(tiles)} tiles')
        for tile in tiles:
            if tile not in tile_to_sources:
                tile_to_sources[tile] = set([])
            tile_to_sources[tile].add(source)
        with open(f'../source-catalog/{source}/metadata.json') as f:
            source_to_metadata[source] = json.load(f)
    
    zoom_to_tiles = {}
    zoom_to_tiles[utils.macrotile_z] = set(tile_to_sources.keys())
    
    for parent_z in range(utils.macrotile_z - 1, coverage_index_minzoom - 1, -1):
        print(f'getting zoom {parent_z} parent tiles...')
        zoom_to_tiles[parent_z] = set([])
        for tile in zoom_to_tiles[parent_z + 1]:
            parent_tile = mercantile.parent(tile, zoom=parent_z)
            zoom_to_tiles[parent_z].add(parent_tile)
            if parent_tile not in tile_to_sources:
                tile_to_sources[parent_tile] = set([])
            tile_to_sources[parent_tile] |= tile_to_sources[tile]
        print(f'found {len(zoom_to_tiles[parent_z])} parents')

    out_filepath = 'meta-store/coverage-index.pmtiles'
    tile_ids = []
    for zoom in zoom_to_tiles.keys():
        print(f'getting tile ids for zoom {zoom}')
        tile_ids.extend([zxy_to_tileid(tile.z, tile.x, tile.y) for tile in zoom_to_tiles[zoom]])
    tile_ids.sort()

    print('writing pmtiles file...')
    with open(out_filepath, 'wb') as f:
        writer = Writer(f)
        for i, tile_id in enumerate(tile_ids):
            if i % 10_000 == 0:
                print(f'{i:_} / {len(tile_ids):_}')
            z, x, y = tileid_to_zxy(tile_id)
            sources = list(tile_to_sources[mercantile.Tile(x=x, y=y, z=z)])
            sources.sort(key=lambda source: source_to_metadata[source]['resolution'])
            data = {}
            for source in sources:
                data[source] = source_to_metadata[source]
            writer.write_tile(tile_id, json.dumps(data).encode('utf-8'))
            
        writer.finalize(
            {
                'tile_type': TileType.UNKNOWN, 
                'tile_compression': Compression.NONE,
                'min_zoom': utils.macrotile_z,
                'max_zoom': utils.macrotile_z,
                'min_lon_e7': 0,
                'min_lat_e7': 0,
                'max_lon_e7': 0,
                'max_lat_e7': 0,
                'center_zoom': 12,
                'center_lon_e7': 0,
                'center_lat_e7': 0,
            },
            {
                'attribution': '<a href="https://mapterhorn.com/attribution">© Mapterhorn</a>'
            },
        )
            
if __name__ == '__main__': 
    create_coverage_pmtiles()
