import sys
import json
import requests

# Some sources have an OGC API Features Collection endpoint to fetch the actual DEM file URLs as
# a GeoJSON Feature Collection.
# Note that these service endpoints provide "paging", so multiple requests may be needed.
# The actual JSON property is not standardized, so needs specification for each endpoint.
# Each source can specify a file "file_list_spec.json" containing the Collections items endpoint
# and specific property attribute for the URLs.
# These endpoints also support a "bbox" parameter (in EPSG:4326 ll-ur lon,lat order) to only acquire download

TIFF_URLS = []

def error_exit(msg):
    print(msg)
    exit(1)


def download_files(url, property):
    print(f'downloading {url}...')
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception('Error: could not get JSON from {url}')

    feat_collection = json.loads(r.text)
    features = feat_collection.get('features', [])
    if len(features) == 0:
        print(f'no features - TIFF_URLS count={len(TIFF_URLS)} - ALL DONE')
        return

    for feature in features:
        if '/' in property:
            attrs = property.split('/')
            if len(attrs) == 3:
                TIFF_URLS.append(feature[attrs[0]][attrs[1]][attrs[2]])
        else:
            TIFF_URLS.append(feature['properties'][property])

    print(f'TIFF_URLS count={len(TIFF_URLS)}...')

    # We may have paged responses: check the link elemments
    links = feat_collection.get('links', [])
    for link in links:
        if link.get('rel', None) == 'next':
            download_files(link.get('href'), property)


def main():
    source = None
    if len(sys.argv) >= 2:
        source = sys.argv[1]
        
    if not source:
        error_exit(f'source={source} {len(sys.argv)} args ignored - {sys.argv[0]} (nl5m|nl50cm|deniedersachsen) e.g. source_get_filel_ist.py nl5m [bbox]')

    # Optional bbox for DEM URLs in specific area
    bbox = None
    if len(sys.argv) == 3:
        bbox = sys.argv[2]

    file_list_spec_file = f'../source-catalog/{source}/file_list_spec.json'
    file_list_spec = json.load(open(file_list_spec_file))
    property = file_list_spec['property']
    urls = file_list_spec['collections']
    for url in urls:
        print(f'TIFF_URLS count={len(TIFF_URLS)} - START: {url}')
        if bbox:
            if '?' in url:
                # URL may have like a ?limit= param
                url = f'{url}&'
            else:
                url = f'{url}?'

            url = f'{url}bbox={bbox}'

        download_files(url, property)

    if len(TIFF_URLS) == 0:
        error_exit('NO TIFF_URLS found')

    outfile = f'../source-catalog/{source}/file_list.txt'
    with open(outfile, 'w') as f:
        for tiff_url in sorted(TIFF_URLS):
            if tiff_url.startswith('http'):
                f.write(f'{tiff_url}\n')


if __name__ == '__main__':
    main()
