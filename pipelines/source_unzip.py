from glob import glob
import sys
import zipfile
import os
import shutil

import utils

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'unzipping {source}...')
    else:
        print('source argument missing...')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))


    for j, filepath in enumerate(filepaths):
        if not zipfile.is_zipfile(filepath):
            continue
        utils.run_command(f'unzip -o {filepath} -d source-store/{source}/tmp/', silent=False)
        utils.run_command(f'rm {filepath}', silent=False)
        tif_filepaths = glob(f'source-store/{source}/tmp/**/*.tif', recursive=True)
        for tif_filepath in tif_filepaths:
            tif_filename = tif_filepath.split('/')[-1]
            utils.run_command(f'mv {tif_filepath} source-store/{source}/{tif_filename}')
        for entry in os.scandir(f'source-store/{source}/'):
            if entry.is_dir():
                shutil.rmtree(entry.path)

if __name__ == '__main__':
    main()
