import sys
import json
import requests

SILENT = False
TIFF_URLS = []


def download_files(url, property):
    print(f'downloading {url}...')
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception('Error: could not get JSON from {url}')

    feat_collection = json.loads(r.text)
    features = feat_collection.get('features', [])
    if len(features) == 0:
        print('no features')
        return

    for feature in features:
        TIFF_URLS.append(feature['properties'][property])
    print(f'TIFF_URLS count={len(TIFF_URLS)})...')

    # We may have paged responses: check the link elemments
    links = feat_collection.get('links', [])
    for link in links:
        if link.get('rel', None) == 'next':
            download_files(links[1].get('href'), property)
            # Ends recursion
            print(f'TIFF_URLS count={len(TIFF_URLS)}) ALL DONE')

def main():
    url = None
    if len(sys.argv) == 4:
        url = sys.argv[1]
        property =sys.argv[2]
        outfile = sys.argv[3]
    else:
        print('url argument missing...')
        exit()

    download_files(url, property)
    with open(outfile, 'w') as f:
        for tiff_url in TIFF_URLS:
            f.write(f'{tiff_url}\n' )

if __name__ == '__main__':
    main()
