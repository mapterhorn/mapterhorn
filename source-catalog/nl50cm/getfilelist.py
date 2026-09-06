import sys
import json
import requests

# Specify resources needed for download URL list
AHN5_ITEMS = 'https://api.ellipsis-drive.com/v3/ogc/features/65945b69-81df-4270-97f0-f029033154c1/collections/01170035-93d3-4a38-b04c-8e7be7a7ca78/items'
AHN6_ITEMS = 'https://api.ellipsis-drive.com/v3/ogc/features/0820faae-5240-499b-8486-cf406433cf71/collections/6aec07f5-f7eb-4f51-b6f7-aee45e5767bd/items'

# Support both AHN 5m and 50cm.
DATASETS = {
    'dtm_50cm':
        {
            'property': 'Maaiveldmodel (DTM) ½m',
            'collections': [AHN5_ITEMS, AHN6_ITEMS]
        },
    'dtm_5m':
        {
            'property': 'Maaiveldmodel (DTM) 5m',
            'collections': [AHN5_ITEMS, AHN6_ITEMS]
        }
}
TIFF_URLS = []


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
        TIFF_URLS.append(feature['properties'][property])
    print(f'TIFF_URLS count={len(TIFF_URLS)}...')

    # We may have paged responses: check the link elemments
    links = feat_collection.get('links', [])
    for link in links:
        if link.get('rel', None) == 'next':
            download_files(links[1].get('href'), property)


def main():
    reso_set = ''
    outfile = None
    if len(sys.argv) == 3:
        reso_set = sys.argv[1]
        outfile = sys.argv[2]

    if not outfile or reso_set not in DATASETS:
        print(f'bad argument(s): {reso_set} {outfile} - getfilelist.py (dtm_50cm|dtm_5m) outfile, e.g. getfilelist.py dtm_50cm file_list.txt')
        exit()

    dataset = DATASETS[reso_set]
    property = dataset['property']
    urls = dataset['collections']
    for url in urls:
        print(f'TIFF_URLS count={len(TIFF_URLS)} - START: {url}')
        download_files(url, property)

    with open(outfile, 'w') as f:
        for tiff_url in sorted(TIFF_URLS):
            if tiff_url.startswith('http'):
                f.write(f'{tiff_url}\n')


if __name__ == '__main__':
    main()
