import os
import sys
import json

import mercantile
from pmtiles.reader import Reader, MmapSource

import bundle

def get_md5sum(filepath):
    checksum = None
    with open(f'{filepath}.md5') as f:
        line = f.readline()        
        parts = line.strip().split(' ')
        assert len(parts) == 2
        checksum = parts[0]
    return checksum

def main():
    version = None
    if len(sys.argv) > 1:
        version = sys.argv[1]
        print(f'start creating download_urls.json for version {version}...')
    else:
        print('version argument missing...')
        exit()

    parent_to_filepaths = bundle.get_parent_to_filepaths()
    parents = parent_to_filepaths.keys()
    names = [bundle.get_name_from_parent(parent) for parent in parents]
    filepaths = [f'bundle-store/{name}/{name}.pmtiles' for name in names]

    data = {
        'version': version,
        'items': []
    }
    for filepath in filepaths:
        min_zoom = None
        max_zoom = None
        with open(filepath , 'r+b') as f2:
            reader = Reader(MmapSource(f2))
            header = reader.header()
            min_zoom = header['min_zoom']
            max_zoom = header['max_zoom']
        filename = filepath.split('/')[-1]
        tile = None
        if filename == 'planet.pmtiles':
            tile = mercantile.Tile(x=0, y=0, z=0)
        else:
            z, x, y = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
            tile = mercantile.Tile(x=x, y=y, z=z)
        
        bounds = mercantile.bounds(tile)
        data['items'].append({
            'name': filename,
            'url':  f'https://download.mapterhorn.com/{filename}',
            'md5sum': get_md5sum(filepath),
            'size': os.path.getsize(filepath),
            'min_lon': bounds.west,
            'min_lat': bounds.south,
            'max_lon': bounds.east,
            'max_lat': bounds.north,
            'min_zoom': min_zoom,
            'max_zoom': max_zoom,
        })
        print(json.dumps(data['items'][-1], indent=2))

    with open('bundle-store/download_urls.json', 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    main()
