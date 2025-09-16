from glob import glob
import sys

import utils

from multiprocessing import Pool

def fix_orientation(filepath):
    utils.run_command(f'mv {filepath} {filepath}.bak')
    utils.run_command(f'gdalwarp -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath}.bak" "{filepath}"', silent=True)
    utils.run_command(f'rm {filepath}.bak')

def main():
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'fixing orientation for source {source}...')
    else:
        print('source argument missing...')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    argument_tuples = []
    for filepath in filepaths:
        if not filepath.endswith('.tif'):
            continue
        argument_tuples.append((filepath,))
    
    with Pool() as pool:
        pool.starmap(fix_orientation, argument_tuples)

if __name__ == '__main__':
    main()
