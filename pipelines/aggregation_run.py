from glob import glob
import shutil
import os
from multiprocessing import Pool

import aggregation_reproject
import aggregation_merge
import aggregation_tile
import utils

def run(filepath):
    filename = filepath.split('/')[-1]
    item = filename.replace('-aggregation.csv', '')
    if os.path.isfile(f'{filepath}.done'):
        print(f'Aggregation item {item} already done. Skipping...')
        return
    print(f'{item} start')
    tmp_folder = f'tmp-store/{item}'
    os.makedirs(tmp_folder, exist_ok=True)
    aggregation_reproject.reproject(filepath, tmp_folder)
    aggregation_merge.merge(filepath, tmp_folder)
    aggregation_tile.main(filepath, tmp_folder)
    # shutil.rmtree(tmp_folder)
    os.rename(f'{filepath}.todo', f'{filepath}.done')
    print(f'{item} end')

def main():
    
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    dirty_filepaths = [filepath.replace('.todo', '') for filepath in glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv.todo')]
    
    if len(dirty_filepaths) == 0:
        print('nothing to do.')
    else:
        print(f'start aggregating {len(dirty_filepaths)} items...')

    argument_tuples = [(dirty_filepath,) for dirty_filepath in dirty_filepaths]
    with Pool() as pool:
        pool.starmap(run, argument_tuples, chunksize=1)

if __name__ == '__main__':
    main()
