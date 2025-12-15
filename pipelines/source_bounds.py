from glob import glob
import sys
import math

import rasterio
from rasterio.warp import transform_bounds

import utils

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'creating bounds for {source}...')
    else:
        print('source argument missing...')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*.tif'))

    bounds_file_lines = ['filename,left,bottom,right,top,width,height\n']

    for j, filepath in enumerate(filepaths):
        with rasterio.open(filepath) as src:
            if src.crs is None:
                raise ValueError(f'crs not defined on {filepath}')
            left, bottom, right, top = transform_bounds(src.crs, 'EPSG:3857', *src.bounds)

            if right - left > 0.9 * 2 * utils.X_MAX_3857:
                # probably the image crosses the antimeridian
                # in this case rasterio.warp.transform_bounds mixes up left and right
                # and we need to flip it back
                left, right = right, left

            for num in [left, bottom, right, top]:
                if not math.isfinite(num):
                    raise ValueError(f'Number in bounds is not finite. src.bounds={src.bounds} src.crs={src.crs} bounds={(left, bottom, right, top)}')
            filename = filepath.split('/')[-1]
            bounds_file_lines.append(f'{filename},{left},{bottom},{right},{top},{src.width},{src.height}\n')
            if j % 100 == 0:
                print(f'{j} / {len(filepaths)}')

    with open(f'source-store/{source}/bounds.csv', 'w') as f:
        f.writelines(bounds_file_lines)

if __name__ == '__main__':
    main()
