import time
from multiprocessing import Process, Pool
import secrets
import os
from glob import glob

import aggregation_run
import downsampling_run

NUM_WORKERS = int(os.environ.get('MAPTERHORN_NUM_WORKERS', 32))

def work_on_task(response_filepath):
    filepath = None
    with open(response_filepath) as f:
        lines = f.readlines()
        assert len(lines) == 1
        filepath = lines[0].strip()
    
    if filepath.endswith('-aggregation.csv'):
        aggregation_run.run(filepath)
    elif filepath.endswith('-downsampling.csv'):
        downsampling_run.downsample_single(filepath)
    else:
        print(f'unknown task {filepath}')
        raise Exception()

def worker():
    print('starting worker...')
    while True:
        task_name = secrets.token_hex(16)
        host_name = os.uname().nodename
        request_filepath = f'task-store/{host_name}-{task_name}.request'
        print('writing request...')
        with open(request_filepath, 'w') as f:
            f.write('')

        response_filepath = request_filepath.replace('.request', '.response')
        
        while not os.path.isfile(response_filepath):
            time.sleep(1)
    
        work_on_task(response_filepath)
        
def run_pool():
    print('starting pool...')
    processes = [Process(target=worker) for _ in range(NUM_WORKERS)]
    for p in processes:
        p.start()
    
    while True:
        dead_processes = []
        for p in processes:
            if not p.is_alive():
                p.join()
                dead_processes.append(p)
        
        for p in dead_processes:
            if p.exitcode != 0:
                for other in processes:
                    if other.is_alive():
                        other.terminate()

                for other in processes:
                    other.join()

                raise Exception()
        
        if len(dead_processes) == len(processes):
            break

        time.sleep(1)

    print('all processes are terminated.')

def complete_assigned_tasks():
    host_name = os.uname().nodename
    response_filepaths = glob(f'task-store/{host_name}-*.response')

    argument_tuples = [(response_filepath,) for response_filepath in response_filepaths]

    with Pool(NUM_WORKERS) as pool:
        pool.starmap(work_on_task, argument_tuples, chunksize=1)
    
if __name__ == '__main__':
    complete_assigned_tasks()
    run_pool()