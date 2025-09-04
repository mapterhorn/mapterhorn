from glob import glob
import sys

import rasterio

import utils
  
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

    for j, filepath in enumerate(filepaths):
        if j % 100 == 0:
            print(f'{j} / {len(filepaths)}')
        if not filepath.endswith('.tif'):
            continue
        with rasterio.open(filepath) as src:
            if src.nodata is None:
                utils.run_command(f'mv {filepath} {filepath}.bak', silent=False)
                utils.run_command(f'gdal_translate {filepath}.bak {filepath} -a_nodata {nodata} -of COG -co COMPRESS=LZW', silent=False)
            
if __name__ == '__main__':
    main()