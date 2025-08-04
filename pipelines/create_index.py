from glob import glob

import mercantile
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

filepaths = glob(f'pmtiles-store/*.pmtiles') + glob(f'pmtiles-store/*/*.pmtiles')
out_filepath = 'index.pmtiles'
with open(out_filepath, 'wb') as f:
    writer = Writer(f)

    for j, filepath in enumerate(filepaths):
        if j % 100 == 0:
            print(f'{j} / {len(filepaths)}')
        filename = filepath.split('/')[-1]
        z, x, y, child_z = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
        children = None
        if z == child_z:
            children = [mercantile.Tile(x=x, y=y, z=z)]
        else:
            children = mercantile.children(mercantile.Tile(x=x, y=y, z=z), zoom=child_z)
        for child in children:
            tile_id = zxy_to_tileid(child.z, child.x, child.y)
            writer.write_tile(tile_id, filepath.replace('pmtiles-store/', '').encode('utf-8'))
    writer.finalize(
        {
            'tile_type': TileType.UNKNOWN, 
            'tile_compression': Compression.NONE,
            'min_zoom': 0,
            'max_zoom': 17,
            'min_lon_e7': 0,
            'min_lat_e7': 0,
            'max_lon_e7': 0,
            'max_lat_e7': 0,
            'center_zoom': 12,
            'center_lon_e7': 0,
            'center_lat_e7': 0,
        },
        {
            'attribution': '<a href="https://github.com/mapterhorn/mapterhorn">© Mapterhorn</a>'
        },
    )

