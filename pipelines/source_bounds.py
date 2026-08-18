from glob import glob
import sys
import math

import rasterio
from rasterio.warp import transform_bounds

import source_marker
import utils

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print('creating bounds for {}...'.format(source))
    else:
        print('source argument missing...')
        exit()

    source_marker.require_download_complete(source)
    
    filepaths = sorted(glob(utils.store_dir('source-store') + '/{}/*.tif'.format(source)))

    bounds_file_lines = ['filename,left,bottom,right,top,width,height\n']

    for j, filepath in enumerate(filepaths):
        with rasterio.open(filepath) as src:
            if src.crs is None:
                raise ValueError('crs not defined on {}'.format(filepath))
            left, bottom, right, top = transform_bounds(src.crs, 'EPSG:3857', *src.bounds)

            if right - left > 0.9 * 2 * utils.X_MAX_3857:
                # probably the image crosses the antimeridian
                # in this case rasterio.warp.transform_bounds mixes up left and right
                # and we need to flip it back
                left, right = right, left

            left, bottom, right, top = utils.clamp_bounds_3857(left, bottom, right, top)

            if right <= left or top <= bottom:
                print('skipping {} after web-mercator clamp (empty extent)'.format(filepath))
                continue

            for num in [left, bottom, right, top]:
                if not math.isfinite(num):
                    raise ValueError('Number in bounds is not finite. src.bounds={} src.crs={} bounds={}'.format(src.bounds, src.crs, (left, bottom, right, top)))
            filename = filepath.split('/')[-1]
            bounds_file_lines.append('{},{},{},{},{},{},{}\n'.format(filename, left, bottom, right, top, src.width, src.height))
            if j % 100 == 0:
                print('{} / {}'.format(j, len(filepaths)))

    with open(utils.store_dir('source-store') + '/{}/bounds.csv'.format(source), 'w') as f:
        f.writelines(bounds_file_lines)

if __name__ == '__main__':
    main()
