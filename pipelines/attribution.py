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

    data = []
    for source in sources:
        with open(f'../source-catalog/{source}/metadata.json') as f:
            metadata = json.load(f)
            data.append({
                'source': source,
                'name': metadata['name'],
                'website': metadata['website'],
                'license': metadata['license'],
                'producer': metadata['producer'],
                'license_pdf': f'https://github.com/mapterhorn/mapterhorn/blob/main/source-catalog/{source}/LICENSE.pdf',
            })

    with open('bundle-store/attribution.json', 'w') as f:
        json.dump(data, f)

if __name__ == '__main__':
    main()
