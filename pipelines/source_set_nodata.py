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
    if len(sys.argv) > 2:
        source = sys.argv[1]
        nodata = sys.argv[2]
        print(f'setting nodata={nodata} for source={source}...')
    else:
        print('arguments missing, usage: python source_set_nodata.py {{source}} {{nodata}} [--force]')
        exit()
    
    if len(sys.argv) == 4:
        if sys.argv[3] == '--force':
            force = True
            print('Force NODATA overwrite on all files.')
        else:
            print('unkown third argument, usage: python source_set_nodata.py {{source}} {{nodata}} [--force]')
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

    print(f'Found these nodata values: {nodata_values}')
    print(f'Will set nodata on {len(argument_tuples)} files. Nothing to do for the remaining {len(filepaths) - len(argument_tuples)} files...')
    with Pool() as pool:
        pool.starmap(set_nodata, argument_tuples)

if __name__ == '__main__':
    main()