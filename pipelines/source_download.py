# /// script
# requires-python = ">=3.13"
# dependencies = [
# ]
#
# [tool.pixi.workspace]
# channels = ["conda-forge"]
# ///

import sys
import os
import subprocess

def run_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout.decode(), stderr.decode()

def download_from_internet(source):
    urls = []
    with open(f'../source-catalog/{source}/file_list.txt') as f:
        urls = [l.strip() for l in f.readlines()]
    j = 0
    for url in urls:
        j += 1
        if j % 100 == 0:
            print(f'downloaded {j} / {len(urls)}')

        command = f'cd source-store/{source} && wget --no-verbose --continue "{url}"'
        run_command(command)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'downloading {source}...')
    else:
        print('source argument missing...')
        exit()

    os.makedirs( f'source-store/{source}/', exist_ok=True)
    download_from_internet(source)

if __name__ == '__main__':
    main()
