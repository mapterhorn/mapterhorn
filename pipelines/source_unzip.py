from glob import glob
import sys
import zipfile
import os
import shutil
from multiprocessing import Pool

import utils

def unzip_tif(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'unzip -o {filepath} -d source-store/{source}/tmp-{filename}/', silent=False)
    utils.run_command(f'rm {filepath}', silent=False)
    tif_filepaths = glob(f'source-store/{source}/tmp-{filename}/**/*.tif', recursive=True)
    for tif_filepath in tif_filepaths:
        tif_filename = tif_filepath.split('/')[-1]
        utils.run_command(f'mv {tif_filepath} source-store/{source}/{tif_filename}')
    shutil.rmtree(f'source-store/{source}/tmp-{filename}/')

def is_7z_head_file(filepath):
    return filepath.endswith('.7z') or filepath.endswith('.7z.001')

def un7z_asc(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'7z x -osource-store/{source}/tmp-{filename}/ "{filepath}"', silent=False)
    asc_filepaths = glob(f'source-store/{source}/tmp-{filename}/**/*.asc', recursive=True)
    for asc_filepath in asc_filepaths:
        tif_filename = asc_filepath.split('/')[-1]
        utils.run_command(f'mv {asc_filepath} source-store/{source}/{tif_filename}')
    for entry in os.scandir(f'source-store/{source}/'):
        if entry.is_dir():
            shutil.rmtree(entry.path)
    filepaths_to_remove = None
    if filepath.endswith('.7z'):
        filepaths_to_remove = [filepath]
    else:
        # it ends with '.7z.001'
        filepaths_to_remove = glob(filepath.replace('.7z.001', '.7z.*'))
    for filepath_to_remove in filepaths_to_remove:
        utils.run_command(f'rm "{filepath_to_remove}"', silent=False)
    shutil.rmtree(f'source-store/{source}/tmp-{filename}/')

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'unzipping {source}...')
    else:
        print('source argument missing...')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    filepaths_zip = []
    filepaths_7z = []
    for filepath in filepaths:
        # so far we only have tif files in zip archives and asc files in 7z archives
        if zipfile.is_zipfile(filepath):
            filepaths_zip.append(filepath)
        elif is_7z_head_file(filepath):
            filepaths_7z.append(filepath)
    
    argument_tuples_zip = []
    for filepath in filepaths_zip:
        argument_tuples_zip.append((filepath, source))
    
    argument_tuples_7z = []
    for filepath in filepaths_7z:
        argument_tuples_7z.append((filepath, source))

    with Pool() as pool:
        pool.starmap(unzip_tif, argument_tuples_zip)
    
    with Pool(1) as pool:
        pool.starmap(un7z_asc, argument_tuples_7z)

if __name__ == '__main__':
    main()
