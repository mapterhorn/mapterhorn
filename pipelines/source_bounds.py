from glob import glob

import math
import rasterio
from rasterio.warp import transform_bounds

import local_config
import utils
    
if __name__ == '__main__':
    remote = f'{local_config.remote_source_store_path}/{local_config.source}/'
    local = f'source-store/{local_config.source}/'

    utils.create_folder(local)

    # utils.rsync(src=remote, dst=local)

    filepaths = sorted(glob(f'source-store/{local_config.source}/*'))

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

    with open(f'source-store/{local_config.source}/bounds.csv', 'w') as f:
        f.writelines(bounds_file_lines)

    # utils.rsync(src=local, dst=remote)
