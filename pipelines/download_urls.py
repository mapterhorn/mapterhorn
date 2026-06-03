import sys
import json

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

    parent_to_filepaths = bundle.get_parent_to_filepaths(only_dirty=False, num_aggregations=1)
    parents = parent_to_filepaths.keys()
    names = [bundle.get_name_from_parent(parent) for parent in parents]

    data = {
        'version': version,
        'items': []
    }
    for name in names:
        meta = None
        with open(f'meta-store/bundle/{name}.json') as f:
            meta = json.load(f)
        
        data['items'].append({
            'name': f'{name}.pmtiles',
            'url':  f'https://download.mapterhorn.com/{name}.pmtiles',
            'md5sum': meta['md5sum'],
            'size': meta['size'],
            'min_lon': meta['min_lon'],
            'min_lat': meta['min_lat'],
            'max_lon': meta['max_lon'],
            'max_lat': meta['max_lat'],
            'min_zoom': meta['min_zoom'],
            'max_zoom': meta['max_zoom'],
        })
        print(json.dumps(data['items'][-1], indent=2))

    with open('meta-store/download_urls.json', 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    main()
