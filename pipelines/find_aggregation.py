import utils

from glob import glob
import mercantile

aggregation_id = utils.get_aggregation_ids()[-1]

filepaths = glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv')

target = mercantile.Tile(x=70583, y=45695, z=17)

for filepath in filepaths:
    filename = filepath.split('/')[-1]
    z, x, y, _ = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]
    tile = mercantile.Tile(x=x, y=y, z=z)
    if mercantile.parent(target, zoom=z) == tile:
        print(filename)
