from glob import glob
import sys

import utils

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

    for j, filepath in enumerate(filepaths):
        if not filepath.endswith('.tif'):
            continue
        if j % 100 == 0:
            print(f'{j} / {len(filepaths)}')
        utils.run_command(f'gdal_edit.py -a_srs {crs} {filepath}')

if __name__ == '__main__':
    main()
