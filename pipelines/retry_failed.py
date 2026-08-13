# Requeue failed aggregation/downsampling items.
from glob import glob
import os
import sys

import utils
import status as status_mod
import log


def retry(kind=None):
    aggregation_ids = utils.get_aggregation_ids()
    if not aggregation_ids:
        print('no aggregation-store found')
        return 0
    aggregation_id = aggregation_ids[-1]
    pattern = utils.store_dir('aggregation-store') + '/{}/*.csv.failed'.format(aggregation_id)
    if kind in ('aggregation', 'downsampling'):
        pattern = utils.store_dir('aggregation-store') + '/{}/*-{}.csv.failed'.format(aggregation_id, kind)
    failed = sorted(glob(pattern))
    count = 0
    for path in failed:
        # foo-aggregation.csv.failed -> foo-aggregation.csv.todo
        todo = path[:-len('.failed')] + '.todo'
        # path is like .../x-aggregation.csv.failed
        # want .../x-aggregation.csv.todo
        base_csv = path.replace('.failed', '')
        if not base_csv.endswith('.csv'):
            # handle *.csv.failed
            pass
        todo_path = path.replace('.failed', '.todo')
        # Also ensure the marker is on the csv: file.csv.todo
        # Existing convention: aggregation.csv.todo alongside aggregation.csv
        csv_path = path[:-len('.failed')]
        todo_path = csv_path + '.todo'
        if os.path.isfile(path):
            os.remove(path)
        with open(todo_path, 'w') as f:
            f.write('')
        count += 1
        log.info('requeued failed item', item=os.path.basename(csv_path))
    status_mod.heartbeat('retry-failed', last_error=None)
    status_mod.refresh()
    print('requeued {} failed item(s)'.format(count))
    return count


def main():
    kind = sys.argv[1] if len(sys.argv) > 1 else None
    retry(kind)


if __name__ == '__main__':
    main()
