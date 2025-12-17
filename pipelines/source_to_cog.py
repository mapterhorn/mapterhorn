from glob import glob
import sys
from multiprocessing import Pool
import utils

SILENT = False

def to_cog(filepath):
    filepath_in = None
    filepath_out = None
    if filepath.endswith('.tif'):
        utils.run_command(f'mv {filepath} {filepath}.bak', silent=SILENT)
        filepath_in = f'{filepath}.bak'
        filepath_out = filepath
    elif filepath.endswith('.TIF'):
        utils.run_command(f'mv {filepath} {filepath}.bak', silent=SILENT)
        filepath_in = f'{filepath}.bak'
        filepath_out = filepath.replace('.TIF', '.tif')
    elif filepath.endswith('.tiff'):
        utils.run_command(f'mv {filepath} {filepath}.bak', silent=SILENT)
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
    
    utils.run_command(f'GDAL_CACHEMAX=512 gdal_translate -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath_in}" "{filepath_out}"', silent=SILENT)
    utils.run_command(f'rm "{filepath_in}"', silent=SILENT)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'converting to cog for source={source}...')
    else:
        print('source argument missing')
        exit()
    
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
        pool.starmap(to_cog, filepaths)
            
if __name__ == '__main__':
    main()