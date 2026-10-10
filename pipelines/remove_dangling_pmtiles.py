from glob import glob
import os

import utils

def main():
    aggregation_id = utils.get_aggregation_ids()[-1]

    filepaths = glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv')
    filepaths += glob(f'aggregation-store/{aggregation_id}/*-downsampling.csv')

    expected_pmtiles_filenames = set([])
    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        expected_pmtiles_filenames.add(filename.replace('-aggregation.csv', '.pmtiles').replace('-downsampling.csv', '.pmtiles'))

    pmtiles_filepaths = set(glob('pmtiles-store/*.pmtiles') + glob('pmtiles-store/*/*.pmtiles'))

    print(f'num expected files: {len(expected_pmtiles_filenames)}')
    print(f'num present files:  {len(pmtiles_filepaths)}')
    for pmtiles_filepath in pmtiles_filepaths:
        pmtiles_filename = pmtiles_filepath.split('/')[-1]
        if pmtiles_filename not in expected_pmtiles_filenames:
            print(f'Removing {pmtiles_filepath}...')
            os.remove(pmtiles_filepath)
    print('done')

if __name__ == '__main__':
    main()