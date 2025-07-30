from glob import glob
import shutil
import time
import datetime

import aggregation_reproject
import aggregation_merge
import aggregation_pmtiles2
import utils

aggregation_ids = utils.get_aggregation_ids()
aggregation_id = aggregation_ids[-1]

filepaths = sorted(glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv'))

batch_size = 128
starts = range(0, len(filepaths), batch_size)

for start in starts:
    print(f'batch {start}:{start + batch_size}. {datetime.datetime.now()}.')
    filepath_batch = filepaths[start:start + batch_size]

    t1 = time.time()
    aggregation_reproject.main(filepath_batch)
    print(f't_reproject: {int(time.time() - t1)} s. {datetime.datetime.now()}.')

    t1 = time.time()
    aggregation_merge.main(filepath_batch)
    print(f't_merge: {int(time.time() - t1)} s. {datetime.datetime.now()}.')

    t1 = time.time()
    aggregation_pmtiles2.main(filepath_batch)
    print(f't_pmtiles: {int(time.time() - t1)} s. {datetime.datetime.now()}.')

    for filepath in filepath_batch:
        tmp_folder = filepath.replace('-aggregation.csv', '-tmp')
        shutil.rmtree(tmp_folder)
