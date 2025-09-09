from glob import glob
import sys
from multiprocessing import Pool
import utils

def to_cog(filepath):
    filepath_in = None
    filepath_out = None
    if filepath.endswith('.tif'):
        utils.run_command(f'mv {filepath} {filepath}.bak', silent=False)
        filepath_in = f'{filepath}.bak'
        filepath_out = filepath
    elif filepath.endswith('.xyz'):
        filepath_in = filepath
        filepath_out = filepath.replace('.xyz', '.tif')
    elif filepath.endswith('.asc'):
        filepath_in = filepath
        filepath_out = filepath.replace('.asc', '.tif')
    
    utils.run_command(f'gdal_translate -of COG -co COMPRESS=LZW -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BLOCKSIZE=512 -co BIGTIFF=YES "{filepath_in}" "{filepath_out}"', silent=True)
    utils.run_command(f'rm "{filepath_in}"', silent=False)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'converting to cog for source={source}...')
    else:
        print('source argument missing')
        exit()
    
    filepaths = [(filepath,) for filepath in sorted(glob(f'source-store/{source}/*.tif') + glob(f'source-store/{source}/*.xyz') + glob(f'source-store/{source}/*.asc'))]

    with Pool() as pool:
        pool.starmap(to_cog, filepaths)
            
if __name__ == '__main__':
    main()