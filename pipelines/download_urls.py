from glob import glob
import os
import sys

import mercantile

import utils


def get_md5sum(filepath):
    out, _ = utils.run_command(f'md5sum {filepath}')
    return out.strip().split('  ')[0]    

def main():
    version = None
    if len(sys.argv) > 1:
        version = sys.argv[1]
        print(f'start creating download_urls.csv for version {version}...')
    else:
        print('version argument missing...')
        exit()

    filepaths = glob('bundle-store/*/*.pmtiles')
    lines = [
        f'version={version}\n',
        'url,name,md5sum,size,min_lon,min_lat,max_lon,max_lat\n'
    ]
    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        tile = None
        if filename == 'planet.pmtiles':
            tile = mercantile.Tile(x=0, y=0, z=0)
        else:
            z, x, y = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
            tile = mercantile.Tile(x=x, y=y, z=z)
        
        md5sum = get_md5sum(filepath)
        size = os.path.getsize(filepath)
        bounds = mercantile.bounds(tile)
        min_lon = bounds.west
        min_lat = bounds.south
        max_lon = bounds.east
        max_lat = bounds.north
        lines.append(f'https://download.mapterhorn.com/{filename},{filename},{md5sum},{size},{min_lon},{min_lat},{max_lon},{max_lat}\n')
        print(lines[-1])

    with open('bundle-store/download_urls.csv', 'w') as f:
        f.writelines(lines)

if __name__ == '__main__':
    main()
