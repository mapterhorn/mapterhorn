from glob import glob
import sys
from multiprocessing import Pool

import rasterio

import utils

def set_nodata(filepath, nodata):
    utils.run_command(f'mv "{filepath}" "{filepath}.bak"', silent=True)
    utils.run_command(f'gdal_translate "{filepath}.bak" "{filepath}" -a_nodata {nodata}', silent=True)
    utils.run_command(f'rm "{filepath}.bak"', silent=True)

def main():
    source = None
    nodata = None
    force = False
    dry_run = False
    if len(sys.argv) > 2:
        source = sys.argv[1]
        nodata = sys.argv[2]
        print(f'source={source}...')
        if '--force' in sys.argv:
            force = True
            print('Force NODATA overwrite on all files...')
        
        if '--dry-run' in sys.argv:
            dry_run = True
            print('Only list existing NODATA values and exit...')
    else:
        print('arguments missing, usage: python source_set_nodata.py {{source}} {{nodata}} [--force] [--dry-run]')
        exit()
        
    filepaths = sorted(glob(f'source-store/{source}/*'))

    argument_tuples = []
    nodata_values = set({})
    for filepath in filepaths:
        if not filepath.endswith('.tif'):
            continue
        with rasterio.open(filepath) as src:
            if src.nodata is None or force:
                argument_tuples.append((filepath, nodata))
            else:
                nodata_values.add(src.nodata)

    print(f'Found these nodata value(s):')
    for v in nodata_values:
        print(f'  {v}')
    print(f'Will set nodata on {len(argument_tuples)} files. Nothing to do for the remaining {len(filepaths) - len(argument_tuples)} files...')
    if dry_run or nodata is None:
        print('This is a dry run. Exit now...')
        return
    with Pool() as pool:
        pool.starmap(set_nodata, argument_tuples, chunksize=1)

if __name__ == '__main__':
    main()