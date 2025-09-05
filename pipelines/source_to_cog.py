from glob import glob
import sys

import rasterio

import utils
  
def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'converting to cog for source={source}...')
    else:
        print('source argument missing')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    for j, filepath in enumerate(filepaths):
        if j % 100 == 0:
            print(f'{j} / {len(filepaths)}')
        if not filepath.endswith('.tif'):
            continue

        utils.run_command(f'mv {filepath} {filepath}.bak', silent=False)
        command = f'gdal_translate {filepath}.bak {filepath} -of COG -co COMPRESS=LZW -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BLOCKSIZE=512 -co BIGTIFF=YES'
        utils.run_command(command, silent=False)
            
if __name__ == '__main__':
    main()