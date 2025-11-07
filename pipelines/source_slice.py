from glob import glob
import sys
import os
from multiprocessing import Pool

import numpy as np
import rasterio

def make_tile(filepath, x, y, w, h):
    with rasterio.open(filepath) as src:
        profile = src.profile
        window = rasterio.windows.Window(x, y, w, h)
        transform = src.window_transform(window)
        tile_profile = profile.copy()
        tile_profile.update({
            'height': h,
            'width': w,
            'transform': transform,
            'compress': 'DEFLATE',
            'tiled': True
        })
        filepath_without_suffix = filepath.replace('.tif', '')
        out_filepath = f'{filepath_without_suffix}_tile_{x}_{y}.tif'
        tile_data = src.read(window=window)
        if np.any(tile_data != src.nodata):
            with rasterio.open(out_filepath, 'w', **tile_profile) as dst:
                dst.write(tile_data)
            print('wrote tile', out_filepath)
        else:
            print('skipping empty tile', out_filepath)

def slice_tif(filepath, tile_size):
    width = None
    height = None

    argument_tuples = []
    with rasterio.open(filepath) as src:
        width = src.width
        height = src.height
        for x in range(0, width, tile_size):
            for y in range(0, height, tile_size):
                w = min(tile_size, width - x)
                h = min(tile_size, height - y)
                argument_tuples.append((filepath, x, y, w, h))
    
    with Pool() as pool:
        pool.starmap(make_tile, argument_tuples, chunksize=1)
    
def main():
    source = None
    tile_size = None
    if len(sys.argv) == 3:
        source = sys.argv[1]
        tile_size = int(sys.argv[2])
        print(f'slicing source {source} into tiles of size {tile_size}...')
    else:
        print('wrong number of arguments: source_slice.py {{source}} {{tile_size}}')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*.tif'))

    for filepath in filepaths:
        slice_tif(filepath, tile_size)
        os.remove(filepath)

if __name__ == '__main__':
    main()






