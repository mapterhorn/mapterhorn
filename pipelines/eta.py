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

# kind = 'aggregation'
kind = 'downsampling'

print('kind', kind)

children_done = count_children(f'-{kind}.done')
children_total = count_children(f'-{kind}.csv')

print('time now:', datetime.now())
print('done, all, percentage:', children_done, children_total, f'{(children_done / children_total):.1%}')

filepaths = glob(f'aggregation-store/{utils.get_aggregation_ids()[-1]}/*-{kind}.done')
if len(filepaths) == 0:
    print('nothing done yet')
    exit()
first_timestamp = math.inf
for filepath in filepaths:
    first_timestamp = min(first_timestamp, os.path.getmtime(filepath))
start_time = datetime.fromtimestamp(first_timestamp)
print('start time:', start_time)
print('eta:', eta(children_done / children_total, start_time))
