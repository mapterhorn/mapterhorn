from glob import glob
import sys

import rasterio
from rasterio.warp import transform_bounds
  
def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'creating bounds for {source}...')
    else:
        print('source argument missing...')
        exit()

    filepaths = sorted(glob(f'source-store/{source}/*'))

    bounds_file_lines = ['filename,left,bottom,right,top,width,height,crs\n']

    for j, filepath in enumerate(filepaths):
        if filepath.endswith('.csv'):
            continue
        with rasterio.open(filepath) as src:
            left, bottom, right, top = transform_bounds(src.crs, 'EPSG:3857', *src.bounds)
            filename = filepath.split('/')[-1]
            bounds_file_lines.append(f'{filename},{left},{bottom},{right},{top},{src.width},{src.height},{src.crs}\n')
            if j % 100 == 0:
                print(f'{j} / {len(filepaths)}')

    with open(f'source-store/{source}/bounds.csv', 'w') as f:
        f.writelines(bounds_file_lines)


if __name__ == '__main__':
    main()
