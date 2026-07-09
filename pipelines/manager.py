from glob import glob
import os

import time

import utils
import downsampling_run

def distribute_tasks(item_filepaths):
    print('start distributing tasks...')
    item_index = 0

    while item_index < len(item_filepaths):
        print(f'{item_index} tasks of {len(item_filepaths)} distributed')
        task_requests = glob('task-store/*.request')
        print(f'received {len(task_requests)} requests')
        for task_request in task_requests:
            task_response = task_request.replace('.request', '.response')
            with open(f'{task_response}.tmp', 'w') as f:
                f.write(item_filepaths[item_index])
            os.replace(f'{task_response}.tmp', task_response)
            os.remove(task_request)
            item_index += 1
            if item_index == len(item_filepaths):
                break
        time.sleep(1)
    
    print('all task are distributed')

def manage_aggregation():
    print('manage aggregation...')

    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    item_filepaths = [filepath.replace('.todo', '') for filepath in glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv.todo')]
    
    if len(item_filepaths) == 0:
        print('nothing to do.')
        return
    else:
        print(f'start distributing {len(item_filepaths)} aggregation items...')


    distribute_tasks(item_filepaths)

    while True:
        all_done = True
        for item_filepath in reversed(item_filepaths):
            if not os.path.isfile(f'{item_filepath}.done'):
                print('not done yet...')
                all_done = False
                break
        if all_done:
            break
        time.sleep(3)
                                     
    
    print('aggregation done.')


def manage_downsampling():

    print('manage aggregation...')

    child_zoom_to_filepaths = downsampling_run.get_child_zoom_to_filepaths()
    child_zooms = list(reversed(sorted(list(child_zoom_to_filepaths.keys()))))
    for child_zoom in child_zooms:
        print(child_zoom)
        print(len(child_zoom_to_filepaths[child_zoom]))
        distribute_tasks(child_zoom_to_filepaths[child_zoom])

        while True:
            all_done = True
            for item_filepath in reversed(child_zoom_to_filepaths[child_zoom]):
                if not os.path.isfile(f'{item_filepath}.done'):
                    print('not done yet...')
                    all_done = False
                    break
            if all_done:
                break
            time.sleep(3)
                                        
        print('downsampling done.')
    
if __name__ == '__main__':
    # manage_aggregation()
    manage_downsampling()
