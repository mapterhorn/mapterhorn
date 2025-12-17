from glob import glob
import sys

import utils

from multiprocessing import Pool
import rasterio

SILENT = False

def set_crs(filepath, crs):
    utils.run_command(f'mv "{filepath}" "{filepath}.bak"', silent=SILENT)
    utils.run_command(f'gdal_translate -a_srs {crs} -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath}.bak" "{filepath}"', silent=SILENT)
    utils.run_command(f'rm "{filepath}.bak"', silent=SILENT)

def main():
    source = None
    crs = None

    if len(sys.argv) == 3:
        source = sys.argv[1]
        crs = sys.argv[2]
        print(f'setting crs="{crs}" for source {source}...')
    elif len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'only listing crses for source {source}...')
    else:
        print('wrong number of arguments: source_set_crs.py source [crs]')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*.tif'))

    if crs is None:
        crses = set({})
        for j, filepath in enumerate(filepaths):
            if j % 100 == 0:
                print(f'{j:_} / {len(filepaths):_}')
            with rasterio.open(filepath) as src:
                crses.add(src.crs)
        print(f'\nfound {len(crses)} crs(es):')
        for crs in crses:
            print(f'  -> {crs}')
        exit()

    argument_tuples = []
    for filepath in filepaths:
        argument_tuples.append((filepath, crs))
    
    with Pool() as pool:
        pool.starmap(set_crs, argument_tuples, chunksize=1)

if __name__ == '__main__':
    main()
