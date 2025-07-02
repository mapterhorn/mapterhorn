import subprocess
from pathlib import Path

import local_config
import utils

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
        utils.run_command(command)

def upload_to_remote_store():
    command = f'rsync -avh {local_config.local_source_store_path}/{local_config.source}/ {local_config.remote_source_store_path}/{local_config.source}/'
    run_command(command)

utils.create_local_store(f'{local_config.local_source_store_path}/{local_config.source}/')

# copy remote source store to local source store
remote = f'{local_config.remote_source_store_path}/{local_config.source}/'
local = f'{local_config.local_source_store_path}/{local_config.source}/'
utils.rsync(src=remote, dst=local)

download_from_internet()

# copy local source store to remote source store
utils.rsync(src=local, dst=remote)
