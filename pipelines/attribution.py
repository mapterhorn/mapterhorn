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

    csv = ['"name","website","license","producer","license_pdf"\n']
    for source in sources:
        with open(f'../source-catalog/{source}/metadata.json') as f:
            metadata = json.load(f)
            line = [metadata['name'], metadata['website'], metadata['license'], metadata['producer'], f'https://github.com/mapterhorn/mapterhorn/blob/main/source-catalog/{source}/LICENSE.pdf']
            line = [entry.replace('"', '""') for entry in line]
            line = [f'"{entry}"' for entry in line]
            csv.append(','.join(line) + '\n')

    with open('bundle-store/attribution.csv', 'w') as f:
        f.writelines(csv)

if __name__ == '__main__':
    main()
