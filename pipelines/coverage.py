from glob import glob

import mercantile

import utils

aggregation_id = utils.get_aggregation_ids()[-1]

filepaths = glob(f'aggregation-store/{aggregation_id}/*aggregation.csv')

source_maxzoom_to_tiles = {}
for filepath in filepaths:
    filename = filepath.split('/')[-1]
    z, x, y, _ = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]
    tile = mercantile.Tile(x=x, y=y, z=z)
    lines = None
    with open(filepath) as f:
        lines = [l.strip() for l in f.readlines()]
    lines = lines[1:]
    for line in lines:
        source, _, maxzoom = line.split(',')
        source_maxzoom = (source, maxzoom)
        if source_maxzoom not in source_maxzoom_to_tiles:
            source_maxzoom_to_tiles[source_maxzoom] = set({})
        source_maxzoom_to_tiles[source_maxzoom].add(tile)

lines = ['x,y,z,source,maxzoom\n']
for source_maxzoom in source_maxzoom_to_tiles:
    source, maxzoom = source_maxzoom
    simplified_tiles = mercantile.simplify(source_maxzoom_to_tiles[source_maxzoom])
    for simplified_tile in simplified_tiles:
        lines.append(f'{simplified_tile.x},{simplified_tile.y},{simplified_tile.z},{source},{maxzoom}\n')

with open('bundle-store/source-coverage.csv', 'w') as f:
    f.writelines(lines)
