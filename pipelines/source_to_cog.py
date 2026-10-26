# /// script
# requires-python = ">=3.13"
# dependencies = [
# ]
#
# [tool.pixi.workspace]
# channels = ["conda-forge"]
#
# [tool.pixi.dependencies]
# gdal = "==3.13.3"
# ///

from glob import glob
import sys
from multiprocessing import Pool
import subprocess

def run_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout.decode(), stderr.decode()

def to_cog(filepath):
    filepath_in = None
    filepath_out = None
    if filepath.endswith('.tif'):
        run_command(f'mv {filepath} {filepath}.bak')
        filepath_in = f'{filepath}.bak'
        filepath_out = filepath
    elif filepath.endswith('.TIF'):
        run_command(f'mv {filepath} {filepath}.bak')
        filepath_in = f'{filepath}.bak'
        filepath_out = filepath.replace('.TIF', '.tif')
    elif filepath.endswith('.tiff'):
        run_command(f'mv {filepath} {filepath}.bak')
        filepath_in = f'{filepath}.bak'
        filepath_out = filepath.replace('.tiff', '.tif')
    elif filepath.endswith('.xyz'):
        filepath_in = filepath
        filepath_out = filepath.replace('.xyz', '.tif')
    elif filepath.endswith('.asc'):
        filepath_in = filepath
        filepath_out = filepath.replace('.asc', '.tif')
    elif filepath.endswith('.ASC'):
        filepath_in = filepath
        filepath_out = filepath.replace('.ASC', '.tif')
    elif filepath.endswith('.txt'):
        filepath_in = filepath
        filepath_out = filepath.replace('.txt', '.tif')
    
    run_command(f'GDAL_CACHEMAX=512 gdal_translate -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath_in}" "{filepath_out}"')
    run_command(f'rm "{filepath_in}"')

def main(source):
    filepaths = []
    filepaths += glob(f'source-store/{source}/*.tif')
    filepaths += glob(f'source-store/{source}/*.TIF')
    filepaths += glob(f'source-store/{source}/*.tiff')
    filepaths += glob(f'source-store/{source}/*.xyz')
    filepaths += glob(f'source-store/{source}/*.asc')
    filepaths += glob(f'source-store/{source}/*.ASC')
    filepaths += glob(f'source-store/{source}/*.txt')

    filepaths = [(filepath,) for filepath in sorted(filepaths)]

    print(f'num files: {len(filepaths)}')
    with Pool() as pool:
        pool.starmap(to_cog, filepaths, chunksize=1)
            
if __name__ == '__main__':
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'converting to cog for source={source}...')
    else:
        print('source argument missing')
        exit()
    main(source)