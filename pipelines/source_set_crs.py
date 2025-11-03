from glob import glob
import sys

import utils

from multiprocessing import Pool

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
    else:
        print('wrong number of arguments: source_set_crs.py source crs')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    argument_tuples = []
    for filepath in filepaths:
        if not filepath.endswith('.tif'):
            continue
        argument_tuples.append((filepath, crs))
    
    with Pool() as pool:
        pool.starmap(set_crs, argument_tuples)

if __name__ == '__main__':
    main()
