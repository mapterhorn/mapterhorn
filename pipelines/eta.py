from glob import glob
from datetime import datetime
import os
import math

import utils

def count_children(suffix):
    aggregation_id = utils.get_aggregation_ids()[-1]
    filepaths = glob(f'aggregation-store/{aggregation_id}/*{suffix}')

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
    print(kind)

    children_done = count_children(f'-{kind}.csv.done')
    if children_done == 0:
        print('not started yet...')
        return
    children_todo = count_children(f'-{kind}.csv.todo')
    children_total = children_done + children_todo

    
    print(f'children_done  = {children_done:_}    ({(children_done / children_total):.1%})\nchildren_total = {children_total:_}')

    filepaths = glob(f'aggregation-store/{utils.get_aggregation_ids()[-1]}/*-{kind}.csv.done')
    if len(filepaths) == 0:
        print('nothing done yet')
    first_timestamp = math.inf
    last_timestamp = -math.inf
    for filepath in filepaths:
        mtime = os.path.getmtime(filepath)
        first_timestamp = min(first_timestamp, mtime)
        last_timestamp = max(last_timestamp, mtime)
    start_time = datetime.fromtimestamp(first_timestamp)
    end_time = datetime.fromtimestamp(last_timestamp)
    
    print('start time:', start_time)
    print('time now:  ', datetime.now())
    if children_done == children_total:
        print('end time:  ', end_time)
    else:
        print('eta:       ', eta(children_done / children_total, start_time))

if __name__ == '__main__':
    compute('aggregation')
    print()
    compute('downsampling')