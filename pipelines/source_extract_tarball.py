import sys
import shutil
from glob import glob
import os
import subprocess

def run_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout.decode(), stderr.decode()

def main(source):
    os.makedirs(f'source-store/{source}', exist_ok=True)
    command = f'tar xf tar-store/{source}.tar -C source-store/{source}'
    run_command(command)

    tif_filepaths = glob(f'source-store/{source}/files/*.tif')
    for tif_filepath in tif_filepaths:
        os.replace(tif_filepath, tif_filepath.replace(f'source-store/{source}/files/', f'source-store/{source}/'))
    
    shutil.rmtree(f'source-store/{source}/files')
    os.remove(f'source-store/{source}/LICENSE.pdf')
    os.remove(f'source-store/{source}/metadata.json')
    os.remove(f'source-store/{source}/coverage.gpkg')

if __name__ == '__main__':
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'extracting tarball of source {source}...')
    else:
        print('Not enough arguments. Usage: source_extract_tarball.py {{source}}')
        exit()
    main(source)
