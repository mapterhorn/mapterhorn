from glob import glob
import json

import utils

def main():
    aggregation_id = utils.get_aggregation_ids()[-1]
    filepaths = glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv')

    sources = set({})
    for filepath in filepaths:
        grouped_source_items = utils.get_grouped_source_items(filepath)
        for source_items in grouped_source_items:
            for source_item in source_items:
                sources.add(source_item['source'])

    sources = sorted(list(sources))
    data = []
    for source in sources:
        item = None
        with open(f'../source-catalog/{source}/metadata.json') as f:
            metadata = json.load(f)
            item = {
                'source': source,
                'name': metadata['name'],
                'website': metadata['website'],
                'license': metadata['license'],
                'producer': metadata['producer'],
                'license_pdf': f'https://github.com/mapterhorn/mapterhorn/blob/main/source-catalog/{source}/LICENSE.pdf',
                'resolution': metadata['resolution'],
                'access_year': metadata['access_year'],
            }      
        meta = None
        with open(f'meta-store/tar/{source}.json') as f:
            meta = json.load(f)
        item['tarball_size'] = meta['size']
        item['tarball_md5sum'] = meta['md5sum']
        item['tarball_url'] = f'https://download.mapterhorn.com/sources/{source}.tar'
        data.append(item)

    with open('meta-store/attribution.json', 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    main()
