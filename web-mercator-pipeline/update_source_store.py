import subprocess
from pathlib import Path

import local_config

def run_command(command):
    print(command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    
    print('stderr')
    print(stderr.decode())
    print('stdout')
    print(stdout.decode())

def create_local_store():
    folder_path = Path(f'{local_config.local_source_store_path}/{local_config.source}/')
    folder_path.mkdir(parents=True, exist_ok=True)

def copy_from_remote_store():
    command = f'rsync -avh {local_config.remote_source_store_path}/{local_config.source}/ {local_config.local_source_store_path}/{local_config.source}/'
    run_command(command)

def download_from_internet():
    urls = []
    with open(f'../source-catalog/{local_config.source}/file_list.txt') as f:
        line = f.readline()
        while line != '':
            urls.append(line.strip())
            line = f.readline()
    for url in urls:
        filename = url.split('/')[-1]
        command = f'wget -O {local_config.local_source_store_path}/{local_config.source}/{filename} -c {url}'
        run_command(command)

def upload_to_remote_store():
    command = f'rsync -avh {local_config.local_source_store_path}/{local_config.source}/ {local_config.remote_source_store_path}/{local_config.source}/'
    run_command(command)

create_local_store()
copy_from_remote_store()
download_from_internet()
upload_to_remote_store()
