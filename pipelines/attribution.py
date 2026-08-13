from glob import glob
import json

import utils

def main():
    aggregation_id = utils.get_aggregation_ids()[-1]
    filepaths = glob(utils.store_dir('aggregation-store') + '/{}/*-aggregation.csv'.format(aggregation_id))

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
        with open(utils.catalog_path(source, 'metadata.json')) as f:
            metadata = json.load(f)
            item = {
                'source': source,
                'name': metadata['name'],
                'website': metadata['website'],
                'license': metadata['license'],
                'producer': metadata['producer'],
                'license_pdf': 'https://github.com/mapterhorn/mapterhorn/blob/main/source-catalog/{}/LICENSE.pdf'.format(source),
                'resolution': metadata['resolution'],
                'access_year': metadata['access_year'],
                'domain': metadata.get('domain', 'land'),
            }
        meta = None
        with open(utils.store_dir('meta-store') + '/tar/{}.json'.format(source)) as f:
            meta = json.load(f)
        item['tarball_size'] = meta['size']
        item['tarball_md5sum'] = meta['md5sum']
        item['tarball_url'] = 'https://download.mapterhorn.com/sources/{}.tar'.format(source)
        data.append(item)

    with open(utils.store_dir('meta-store') + '/attribution.json', 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    main()
