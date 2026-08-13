import time
from multiprocessing import Process, Pool
import secrets
import os
from glob import glob
import traceback

import aggregation_run
import downsampling_run
import log
import status as status_mod
import utils

NUM_WORKERS = int(os.environ.get('MAPTERHORN_NUM_WORKERS', 32))
# If 1, a worker crash kills the whole pool (legacy). Default 0 = continue.
ABORT_ON_WORKER_FAILURE = os.environ.get('MAPTERHORN_ABORT_ON_WORKER_FAILURE', '0') == '1'

def work_on_task(response_filepath):
    filepath = None
    with open(response_filepath) as f:
        lines = f.readlines()
        assert len(lines) == 1
        filepath = lines[0].strip()
    
    try:
        if filepath.endswith('-aggregation.csv'):
            aggregation_run.run(filepath)
        elif filepath.endswith('-downsampling.csv'):
            downsampling_run.downsample_single(filepath)
        else:
            print('unknown task {}'.format(filepath))
            raise Exception('unknown task {}'.format(filepath))
    except Exception as e:
        # aggregation_run/downsampling_run already mark .failed for most errors;
        # this catches unexpected escapes so the worker process stays alive.
        log.error('worker task failed', filepath=filepath, error=str(e))
        failed = '{}.failed'.format(filepath)
        if not os.path.isfile(failed):
            with open(failed, 'w') as f:
                f.write(str(e) + '\n' + traceback.format_exc())
        todo = '{}.todo'.format(filepath)
        if os.path.isfile(todo):
            os.remove(todo)
        status_mod.heartbeat('worker', last_error=str(e)[:500])

def worker():
    print('starting worker...')
    while True:
        task_name = secrets.token_hex(16)
        host_name = os.uname().nodename
        request_filepath = utils.store_dir('task-store') + '/{}-{}.request'.format(host_name, task_name)
        with open(request_filepath, 'w') as f:
            f.write('')

        response_filepath = request_filepath.replace('.request', '.response')
        
        sleep_iterations = 0
        while not os.path.isfile(response_filepath):
            time.sleep(1)
            sleep_iterations += 1
            if sleep_iterations == 600:
                os.remove(request_filepath)
                print('task response timed out. terminating...')
                return
    
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
                log.error('worker process exited', exitcode=p.exitcode)
                status_mod.heartbeat('worker', last_error='worker exitcode {}'.format(p.exitcode))
                if ABORT_ON_WORKER_FAILURE:
                    for other in processes:
                        if other.is_alive():
                            other.terminate()
                    for other in processes:
                        other.join()
                    raise Exception('worker failed')
                # Restart a replacement worker so the pool keeps draining tasks
                replacement = Process(target=worker)
                replacement.start()
                processes.append(replacement)
        
        # Drop finished processes from the list
        processes = [p for p in processes if p.is_alive()]
        
        if len(processes) == 0:
            break

        time.sleep(1)

    print('all processes are terminated.')

def complete_assigned_tasks():
    host_name = os.uname().nodename
    response_filepaths = glob(utils.store_dir('task-store') + '/{}-*.response'.format(host_name))

    argument_tuples = [(response_filepath,) for response_filepath in response_filepaths]

    with Pool(NUM_WORKERS) as pool:
        pool.starmap(work_on_task, argument_tuples, chunksize=1)
    
if __name__ == '__main__':
    complete_assigned_tasks()
    run_pool()
