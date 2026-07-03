from glob import glob
import sys
import math

import rasterio
from rasterio.warp import transform, transform_bounds

import utils

def get_resolutions_3857(src):
    # Returns the true resolution of an image in EPSG:3857 units, measured at
    # the raster center by reprojecting one pixel step in each axis direction.
    # For an image whose CRS is rotated against web mercator, like polar
    # stereographic, this is smaller than bounding box size / pixel count,
    # because the bounding box of a rotated image is inflated by up to sqrt(2).
    center_col = src.width / 2
    center_row = src.height / 2
    x0, y0 = src.transform * (center_col, center_row)
    x1, y1 = src.transform * (center_col + 1, center_row)
    x2, y2 = src.transform * (center_col, center_row + 1)
    xs, ys = transform(src.crs, 'EPSG:3857', [x0, x1, x2], [y0, y1, y2])
    dxs = []
    for x in [xs[1], xs[2]]:
        dx = abs(x - xs[0])
        # a pixel step across the antimeridian wraps around in x
        dxs.append(min(dx, 2 * utils.X_MAX_3857 - dx))
    resolution_x = math.hypot(dxs[0], ys[1] - ys[0])
    resolution_y = math.hypot(dxs[1], ys[2] - ys[0])
    return resolution_x, resolution_y

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

            # width and height are effective pixel counts, chosen such that
            # bounding box size / count gives the true resolution. With the raw
            # raster counts, get_smallest_overzoom in aggregation_covering.py
            # would overestimate the resolution of rotated images and could
            # pick a maxzoom which does not fully resolve them.
            resolution_x, resolution_y = get_resolutions_3857(src)
            for num in [resolution_x, resolution_y]:
                if not math.isfinite(num) or num <= 0:
                    raise ValueError(f'Resolution is not finite and positive. src.crs={src.crs} resolutions={(resolution_x, resolution_y)}')
            width_3857 = right - left if left < right else left - right
            width = max(1, round(width_3857 / resolution_x))
            height = max(1, round((top - bottom) / resolution_y))

            filename = filepath.split('/')[-1]
            bounds_file_lines.append(f'{filename},{left},{bottom},{right},{top},{width},{height}\n')
            if j % 100 == 0:
                print(f'{j} / {len(filepaths)}')

    with open(f'source-store/{source}/bounds.csv', 'w') as f:
        f.writelines(bounds_file_lines)

if __name__ == '__main__':
    main()
