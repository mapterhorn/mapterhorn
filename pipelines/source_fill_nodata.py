import os
import shutil
from glob import glob
from multiprocessing import Pool
import sys

import mercantile
import rasterio

import utils
from aggregation_covering import bounds_intersect, get_intersecting_tiles_dfs

SILENT = True

def get_filled_source(source):
    return f'{source}filled'

def plan_tasks(source):
    filepath = f'source-store/{source}/bounds.csv'
    lines = []
    with open(filepath) as f:
        lines = f.readlines()
    
    if len(lines) - 1 == len(glob(f'source-store/{get_filled_source(source)}/*.csv')):
        print('planning already done...')
        return
    
    print(f'reading {filepath}...')
    bounds_to_filename = {}
    tile_to_bounds_list = {}
    for line in lines[1:]:
        filename, left, bottom, right, top, width, height = line.split(',')
        width, height = [int(a) for a in [width, height]]
        left, bottom, right, top = [float(a) for a in [left, bottom, right, top]]
        buffer = 2 * utils.macrotile_buffer_3857
        bounds = (
            left - buffer,
            bottom - buffer,
            right + buffer,
            top + buffer
        )
        bounds_to_filename[bounds] = filename
        tiles = get_intersecting_tiles_dfs(bounds, mercantile.Tile(x=0, y=0, z=0), 10)
        for tile in tiles:
            if tile not in tile_to_bounds_list:
                tile_to_bounds_list[tile] = []
            tile_to_bounds_list[tile].append(bounds)
    
    print('finding neighbors...')
    filename_to_filenames = {}
    for bounds_list in tile_to_bounds_list.values():
        for i in range(len(bounds_list)):
            for j in range(len(bounds_list)):
                if bounds_intersect(bounds_list[i], bounds_list[j]):
                    filename = bounds_to_filename[bounds_list[i]]
                    if filename not in filename_to_filenames:
                        filename_to_filenames[filename] = set([])
                    filename_to_filenames[filename].add(bounds_to_filename[bounds_list[j]])

    print('writing tasks...')
    folder = f'source-store/{get_filled_source(source)}'
    os.makedirs(folder, exist_ok=True)
    for filename in sorted(filename_to_filenames.keys()):
        task_filepath = f'{folder}/{filename}.csv'
        with open(task_filepath, 'w') as f:
            f.write('\n'.join(sorted(filename_to_filenames[filename])))

def fill_nodata_single(i, source, filename):
    if i % 10 == 0:
        print(f'{i:_}')

    tmp_folder = f'source-store/{get_filled_source(source)}/{filename}_tmp'
    os.makedirs(tmp_folder, exist_ok=True)

    filepath = f'source-store/{source}/{filename}'
    vrt_filepath = f'{tmp_folder}/mosaic.vrt'
    vrt_buffered_filepath = f'{tmp_folder}/mosaic_buffered.vrt'
    tif_filled_filepath = f'{tmp_folder}/filled.tif'
    vrt_cropped_filepath = f'{tmp_folder}/cropped.vrt'
    out_filepath = f'source-store/{get_filled_source(source)}/{filename}'
    if os.path.isfile(f'{out_filepath}.done'):
        return

    tif_filenames = []
    with open(f'source-store/{get_filled_source(source)}/{filename}.csv') as f:
        tif_filenames = [line.strip() for line in f.readlines()]

    input_file_list_path = f'{tmp_folder}/vrt-file-list.txt'
    with open(input_file_list_path, 'w') as f:
        f.write('\n'.join([f'source-store/{source}/{tif_filename}' for tif_filename in tif_filenames]))

    command = f'gdalbuildvrt -overwrite -input_file_list {input_file_list_path} {vrt_filepath}'
    utils.run_command(command, silent=SILENT)

    with rasterio.open(filepath) as src:
        xs, ys = rasterio.warp.transform(src.crs, 'EPSG:3857', [src.bounds.left], [src.bounds.bottom])
        ls, bs = rasterio.warp.transform('EPSG:3857', src.crs, [xs[0] - 2 * utils.macrotile_buffer_3857], [ys[0] - 2 * utils.macrotile_buffer_3857])

        buffer_horizontal = src.bounds.left - ls[0]
        buffer_vertical = src.bounds.bottom - bs[0]

        step_horizontal = (src.bounds.right - src.bounds.left) / src.width
        step_vertical = (src.bounds.top - src.bounds.bottom) / src.height

        buffer_pixels_horizontal = int(buffer_horizontal / step_horizontal)
        buffer_pixels_vertical = int(buffer_vertical / step_vertical)

        left   = src.bounds.left   - buffer_pixels_horizontal * step_horizontal
        right  = src.bounds.right  + buffer_pixels_horizontal * step_horizontal
        bottom = src.bounds.bottom - buffer_pixels_vertical   * step_vertical
        top    = src.bounds.top    + buffer_pixels_vertical   * step_vertical

        command = 'gdalwarp -of vrt -overwrite '
        command += f'-tr {step_horizontal} {step_vertical} '
        command += f'-te {left} {bottom} {right} {top} '
        command += '-dstnodata -9999 '
        command += f'{vrt_filepath} {vrt_buffered_filepath}'
        utils.run_command(command, silent=SILENT)

        max_distance = int(min(buffer_pixels_horizontal, buffer_pixels_vertical) * 0.5)
        command = f'gdal_fillnodata.py -overwrite -md {max_distance} {vrt_buffered_filepath} {tif_filled_filepath}'
        utils.run_command(command, silent=SILENT)

        command = 'gdalwarp -of vrt -overwrite '
        command += f'-tr {step_horizontal} {step_vertical} '
        command += f'-te {src.bounds.left} {src.bounds.bottom} {src.bounds.right} {src.bounds.top} '
        command += '-dstnodata -9999 '
        command += f'{tif_filled_filepath} {vrt_cropped_filepath}'
        utils.run_command(command, silent=SILENT)

        command = 'gdal_translate -of COG -co BIGTIFF=IF_NEEDED -co ADD_ALPHA=YES -co OVERVIEWS=NONE '
        command += '-co SPARSE_OK=YES -co BLOCKSIZE=512 -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 '
        command += f'{vrt_cropped_filepath} '
        command += f'{out_filepath}'
        utils.run_command(command, silent=SILENT)

        with open(f'{out_filepath}.done', 'w') as f:
            f.write('')

    shutil.rmtree(tmp_folder)

def fill_nodata(source):
    filepaths = glob(f'source-store/{get_filled_source(source)}/*.csv')
    print(f'fill nodata in {len(filepaths):_} files...')
    filenames = [filepath.split('/')[-1].replace('.csv', '') for filepath in filepaths]
    argument_tuples = [(i, source, filename) for i, filename in enumerate(filenames)]
    with Pool() as pool:
        pool.starmap(fill_nodata_single, argument_tuples, chunksize=1)

def cleanup(source):
    print('removing tasks and done markers...')
    filepaths = glob(f'source-store/{get_filled_source(source)}/*')
    for filepath in filepaths:
        if filepath.endswith('.csv') or filepath.endswith('.done'):
            os.remove(filepath)

if __name__ == '__main__':
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'filling nodata for {source}...')
    else:
        print('source argument missing...')
        exit()

    plan_tasks(source)
    fill_nodata(source)
    cleanup(source)

    print('done')