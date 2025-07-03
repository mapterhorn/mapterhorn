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

if __name__ == '__main__':
    remote = f'{local_config.remote_source_store_path}/{local_config.source}/'
    local = f'source-store/{local_config.source}/'

    utils.create_local_store(local)

    utils.rsync(src=remote, dst=local)

    download_from_internet()

    utils.rsync(src=local, dst=remote)
