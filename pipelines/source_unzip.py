from glob import glob
import sys
import zipfile
import os
import shutil
from multiprocessing import Pool

import utils

def unzip_tif(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'unzip -o {filepath} -d source-store/{source}/{filename}-tmp/', silent=False)
    utils.run_command(f'rm {filepath}', silent=False)
    tif_filepaths = glob(f'source-store/{source}/{filename}-tmp/**/*.tif', recursive=True)
    for tif_filepath in tif_filepaths:
        tif_filename = tif_filepath.split('/')[-1]
        utils.run_command(f'mv {tif_filepath} source-store/{source}/{tif_filename}')
    shutil.rmtree(f'source-store/{source}/{filename}-tmp/')

def is_7z_head_file(filepath):
    return filepath.endswith('.7z') or filepath.endswith('.7z.001')

def asc_to_cog(filepath_in, filepath_out):
    utils.run_command(f'gdal_translate -of COG -co COMPRESS=LZW -co BLOCKSIZE=512 {filepath_in} {filepath_out}', silent=True)

def un7z_asc(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'7z x -osource-store/{source}/{filename}-tmp/ "{filepath}"', silent=False)
    asc_filepaths = glob(f'source-store/{source}/{filename}-tmp/**/*.asc', recursive=True)
    
    print(f'translating {len(asc_filepaths)} asc files to geotiff...')
    argument_tuples = []
    for asc_filepath in asc_filepaths:
        asc_filename = asc_filepath.split('/')[-1]
        argument_tuples.append((asc_filepath, f'source-store/{source}/{asc_filename.replace(".asc", ".tif")}'))
    with Pool() as pool:
        pool.starmap(asc_to_cog, argument_tuples)

    filepaths_to_remove = None
    if filepath.endswith('.7z'):
        filepaths_to_remove = [filepath]
    else:
        # it ends with '.7z.001'
        filepaths_to_remove = glob(filepath.replace('.7z.001', '.7z.*'))
    for filepath_to_remove in filepaths_to_remove:
        utils.run_command(f'rm "{filepath_to_remove}"', silent=False)
    shutil.rmtree(f'source-store/{source}/{filename}-tmp/')

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
    
    for filepath in filepaths_zip:
        unzip_tif(filepath, source)
    
    for filepath in filepaths_7z:
        un7z_asc(filepath, source)

if __name__ == '__main__':
    main()
