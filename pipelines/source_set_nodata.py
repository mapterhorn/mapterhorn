from glob import glob
import sys
from multiprocessing import Pool

import rasterio

import utils

def set_nodata(filepath, nodata):
    utils.run_command(f'mv "{filepath}" "{filepath}.bak"', silent=True)
    utils.run_command(f'gdal_translate "{filepath}.bak" "{filepath}" -a_nodata {nodata} -of COG -co COMPRESS=LZW', silent=True)
    utils.run_command(f'rm "{filepath}.bak"', silent=True)

def main():
    source = None
    nodata = None
    if len(sys.argv) > 2:
        source = sys.argv[1]
        nodata = sys.argv[2]
        print(f'setting nodata={nodata} for source={source}...')
    else:
        print('arguments missing, usage: python source_assign_nodata.py {{source}} {{nodata}}')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    argument_tuples = []
    for filepath in filepaths:
        if not filepath.endswith('.tif'):
            continue
        with rasterio.open(filepath) as src:
            if src.nodata is None:
                argument_tuples.append((filepath, nodata))

    print(len(argument_tuples))
    with Pool() as pool:
        pool.starmap(set_nodata, argument_tuples)

if __name__ == '__main__':
    main()