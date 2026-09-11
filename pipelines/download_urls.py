import sys
import json
import mercantile

import bundle

def main():
    version = None
    if len(sys.argv) > 1:
        version = sys.argv[1]
        print(f'start creating download_urls.json for version {version}...')
    else:
        print('version argument missing...')
        exit()

    parent_to_filepaths = bundle.get_parent_to_filepaths(num_aggregations=-1)
    parents = parent_to_filepaths.keys()
    names = [bundle.get_name_from_parent(parent) for parent in parents]

    data = {
        'version': version,
        'items': []
    }
    for name in names:
        print(f'working on {name}...')
        meta = None
        with open(f'meta-store/bundle/{name}.json') as f:
            meta = json.load(f)

        tile = None
        if name == 'planet':
            tile = mercantile.Tile(x=0, y=0, z=0)
        else:
            z, x, y = [int(a) for a in name.split('-')]
            tile = mercantile.Tile(x=x, y=y, z=z)
        
        bounds = mercantile.bounds(tile)

        data['items'].append({
            'name': f'{name}.pmtiles',
            'url':  f'https://download.mapterhorn.com/{name}.pmtiles',
            'md5sum': meta['md5sum'],
            'size': meta['size'],
            'min_lon': bounds.west,
            'min_lat': bounds.south,
            'max_lon': bounds.east,
            'max_lat': bounds.north,
            'min_zoom': meta['min_zoom'],
            'max_zoom': meta['max_zoom'],
        })
        print(json.dumps(data['items'][-1], indent=2))

    with open('meta-store/download_urls.json', 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    main()
