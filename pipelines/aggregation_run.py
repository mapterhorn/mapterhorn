from glob import glob
import shutil
import time
import datetime
import os

import aggregation_reproject
import aggregation_merge
import aggregation_tile
import utils



def main():    
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    dirty_filepaths = None
    if len(aggregation_ids) < 2:
        dirty_filepaths = sorted(glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv'))
    else:
        last_aggregation_id = aggregation_ids[-2]
        dirty_filepaths = [f'aggregation-store/{aggregation_id}/{filename}' for filename in utils.get_dirty_aggregation_filenames(aggregation_id, last_aggregation_id)]
    
    dirty_filepaths = [filepath for filepath in dirty_filepaths if not os.path.isfile(filepath.replace('-aggregation.csv', '-aggregation.done'))]
    if len(dirty_filepaths) == 0:
        print('nothing to do.')
    else:
        print(f'start aggregating {len(dirty_filepaths)} items...')

    batch_size = 128
    starts = range(0, len(dirty_filepaths), batch_size)

    for start in starts:
        print(f'batch {start}:{start + batch_size}. {datetime.datetime.now()}.')
        filepath_batch = dirty_filepaths[start:start + batch_size]

        t1 = time.time()
        aggregation_reproject.main(filepath_batch)
        print(f't_reproject: {int(time.time() - t1)} s. {datetime.datetime.now()}.')

        t1 = time.time()
        aggregation_merge.main(filepath_batch)
        print(f't_merge: {int(time.time() - t1)} s. {datetime.datetime.now()}.')

        t1 = time.time()
        aggregation_tile.main(filepath_batch)
        print(f't_pmtiles: {int(time.time() - t1)} s. {datetime.datetime.now()}.')

        for filepath in filepath_batch:
            tmp_folder = filepath.replace('-aggregation.csv', '-tmp')
            shutil.rmtree(tmp_folder)
            utils.run_command(f'touch {filepath.replace("-aggregation.csv", "-aggregation.done")}')

if __name__ == '__main__':
    main()
