import time
from multiprocessing import Process
import secrets
import os

import aggregation_run
import downsampling_run

def worker():
    print('starting worker...')
    while True:
        task_name = secrets.token_hex(16)
        filename = f'task-store/{task_name}.request'
        with open(filename, 'w') as f:
            f.write('')
        
        sleep_iterations = 0
        while not os.path.isfile(f'task-store/{task_name}.response'):
            time.sleep(1)
            sleep_iterations += 1
            if sleep_iterations == 600:
                os.remove(f'task-store/{task_name}.request')
                print('task response timed out. terminating...')
                return
        
        filepath = None
        with open(f'task-store/{task_name}.response') as f:
            lines = f.readlines()
            assert len(lines) == 1
            filepath = lines[0].strip()
        
        os.remove(f'task-store/{task_name}.response')

        if filepath.endswith('-aggregation.csv'):
            aggregation_run.run(filepath)
        elif filepath.endswith('-downsampling.csv'):
            downsampling_run.downsample_single(filepath)
        else:
            print(f'unknown task {filepath}')
            raise Exception()
  
def run_pool():
    print('starting pool...')
    processes = [Process(target=worker) for _ in range(32)]
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

if __name__ == '__main__':
    run_pool()