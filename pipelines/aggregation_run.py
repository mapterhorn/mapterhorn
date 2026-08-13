from glob import glob
import shutil
import os
from multiprocessing import Pool
import time
import traceback

import aggregation_reproject
import aggregation_merge
import aggregation_tile
import utils
import log
import status as status_mod

HEARTBEAT_EVERY = int(os.environ.get('MAPTERHORN_HEARTBEAT_EVERY', '10'))


def mark_failed(filepath, err):
    todo = '{}.todo'.format(filepath)
    failed = '{}.failed'.format(filepath)
    with open(failed, 'w') as f:
        f.write(str(err))
        f.write('\n')
        f.write(traceback.format_exc())
    if os.path.isfile(todo):
        os.remove(todo)
    status_mod.heartbeat('aggregation', last_error=str(err)[:500])


def run(filepath):
    filename = filepath.split('/')[-1]
    item = filename.replace('-aggregation.csv', '')
    if os.path.isfile('{}.done'.format(filepath)):
        log.info('aggregation item already done', item=item)
        return
    if os.path.isfile('{}.failed'.format(filepath)):
        log.warn('aggregation item previously failed; skip (use retry_failed.py)', item=item)
        return
    log.info('aggregation start', item=item)
    try:
        queue_folder = utils.store_dir('tmp-store') + '/queue'
        os.makedirs(queue_folder, exist_ok=True)
        shutil.copy(filepath, '{}/{}.tmp'.format(queue_folder, filename))
        os.rename('{}/{}.tmp'.format(queue_folder, filename), '{}/{}'.format(queue_folder, filename))
        ready_folder = utils.store_dir('tmp-store') + '/ready'
        os.makedirs(ready_folder, exist_ok=True)
        while not os.path.isfile('{}/{}'.format(ready_folder, filename)):
            print('waiting for download...')
            time.sleep(1)
        tmp_folder = utils.store_dir('tmp-store') + '/{}'.format(item)
        os.makedirs(tmp_folder, exist_ok=True)
        tic = time.time()
        print('start reproject...')
        aggregation_reproject.reproject(filepath, tmp_folder)
        print('reproject done in {:.2f} s'.format(time.time() - tic))
        tic = time.time()
        print('start merge...')
        aggregation_merge.merge(filepath, tmp_folder)
        print('merge done in {:.2f} s'.format(time.time() - tic))
        tic = time.time()
        print('start tile...')
        aggregation_tile.main(filepath, tmp_folder)
        print('tile done in {:.2f} s'.format(time.time() - tic))
        shutil.rmtree(tmp_folder)
        os.rename('{}.todo'.format(filepath), '{}.done'.format(filepath))

        os.remove('{}/{}'.format(ready_folder, filename))
        log.info('aggregation end', item=item)
    except Exception as e:
        log.error('aggregation failed', item=item, error=str(e))
        mark_failed(filepath, e)
        # Do not re-raise: allow the pool to continue other items
        return


def main():
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    dirty_filepaths = [filepath.replace('.todo', '') for filepath in glob(utils.store_dir('aggregation-store') + '/{}/*-aggregation.csv.todo'.format(aggregation_id))]
    
    if len(dirty_filepaths) == 0:
        print('nothing to do.')
        status_mod.heartbeat('aggregation', message='nothing to do')
        status_mod.refresh()
        return

    log.info('start aggregating', count=len(dirty_filepaths))
    status_mod.heartbeat('aggregation', aggregation={'started_at': status_mod.utc_now(), 'items_todo': len(dirty_filepaths)})

    def _run_with_heartbeat(filepath):
        run(filepath)
        # Cheap periodic status refresh from workers is noisy; refresh in parent via callback is hard with Pool.
        # Workers update last_error on failure; parent refreshes at end.

    argument_tuples = [(dirty_filepath,) for dirty_filepath in dirty_filepaths]
    with Pool() as pool:
        # Process in chunks so we can heartbeat between batches
        chunk = max(HEARTBEAT_EVERY, 1)
        for i in range(0, len(argument_tuples), chunk):
            batch = argument_tuples[i:i + chunk]
            pool.starmap(run, batch, chunksize=1)
            status_mod.refresh()
            status_mod.heartbeat('aggregation')

    status_mod.refresh()
    status_mod.heartbeat('aggregation-complete')
    log.info('aggregation stage complete')

if __name__ == '__main__':
    main()
