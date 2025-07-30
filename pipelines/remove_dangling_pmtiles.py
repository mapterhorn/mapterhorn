from glob import glob

import utils

aggregation_id = utils.get_aggregation_ids()[-1]

filepaths = glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv')
filepaths += glob(f'aggregation-store/{aggregation_id}/*-downsampling.csv')

expected_pmtiles_filenames = []
for filepath in filepaths:
    filename = filepath.split('/')[-1]
    expected_pmtiles_filenames.append(filename.replace('-aggregation.csv', '.pmtiles').replace('-downsampling.csv', '.pmtiles'))

pmtiles_filepaths = glob(f'pmtiles-store/*.pmtiles')
for pmtiles_filepath in pmtiles_filepaths:
    pmtiles_filename = pmtiles_filepath.split('/')[-1]
    if pmtiles_filename not in expected_pmtiles_filenames:
        utils.run_command(f'rm {pmtiles_filepath}', silent=False)
