from glob import glob
from datetime import datetime
import os
import math

import utils
import status as status_mod

def count_children(suffix):
    aggregation_id = utils.get_aggregation_ids()[-1]
    filepaths = glob(utils.store_dir('aggregation-store') + '/{}/*{}'.format(aggregation_id, suffix))

    total_children = 0
    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        z, x, y, child_z = [int(a) for a in filename.replace(suffix, '').split('-')]
        num_children = 2 ** (2 * (child_z - z))
        total_children += num_children
    return total_children

def eta(progress, start_time):
    now = datetime.now()
    elapsed = now - start_time
    total_duration = elapsed / progress
    eta = start_time + total_duration
    return eta

def compute(kind):
    print()
    print(kind)

    children_done = count_children('-{}.csv.done'.format(kind))
    children_total = count_children('-{}.csv'.format(kind))

    print('time now:', datetime.now())
    if children_total == 0:
        print('no items')
        return
    print('done, all, percentage:', children_done, children_total, '{:.1%}'.format(children_done / children_total))

    filepaths = glob(utils.store_dir('aggregation-store') + '/{}/*-{}.csv.done'.format(utils.get_aggregation_ids()[-1], kind))
    if len(filepaths) == 0:
        print('nothing done yet')
        return
    first_timestamp = math.inf
    for filepath in filepaths:
        first_timestamp = min(first_timestamp, os.path.getmtime(filepath))
    start_time = datetime.fromtimestamp(first_timestamp)
    print('start time:', start_time)
    if children_done > 0:
        print('eta:', eta(children_done / children_total, start_time))

if __name__ == '__main__':
    # Prefer the unified status command; keep this script for quick ETAs
    compute('aggregation')
    compute('downsampling')
    print()
    status_mod.print_status(status_mod.refresh())
