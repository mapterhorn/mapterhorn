# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "rasterio>=1.3.11",
#   "mercantile>=1.2.1",
#   "numpy>=1.24.4",
# ]
#
# [tool.pixi.workspace]
# channels = ["conda-forge"]
#
# [tool.pixi.dependencies]
# gdal = "==3.13.3"
# ///

import json
import os
import subprocess

import rasterio
import mercantile
import numpy as np

MACROTILE_BUFFER_3857 = 150

FAIL_ON_WARNING = False

def run_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout.decode(), stderr.decode()

def create_virtual_raster(tmp_folder, i, source_items):
    source = source_items[0]['source']
    vrt_filepath = f'{tmp_folder}/{i}.vrt'
    input_file_list_path = f'{tmp_folder}/{i}-file-list.txt'
    with open(input_file_list_path, 'w') as f:
        for source_item in source_items:
            f.write(f'tmp-store/source/{source}/{source_item["filename"]}\n')
    command = f'gdalbuildvrt -overwrite -input_file_list {input_file_list_path} {vrt_filepath}'
    out, err = run_command(command)

    if 'heterogeneous projection' in err:
        raise Exception(f'heterogenous projection found in {tmp_folder}')

    return vrt_filepath

def get_resolution(zoom):
    tile = mercantile.Tile(x=0, y=0, z=zoom)
    bounds = mercantile.xy_bounds(tile)
    return (bounds.right - bounds.left) / 512

def create_warp(vrt_filepath, vrt_3857_filepath, zoom, aggregation_tile, buffer):
    left, bottom, right, top = mercantile.xy_bounds(aggregation_tile)
    left -= buffer
    bottom -= buffer
    right += buffer
    top += buffer
    resolution = get_resolution(zoom)
    command = 'gdalwarp -of vrt -overwrite '
    command += '-t_srs EPSG:3857 '
    command += f'-tr {resolution} {resolution} '
    command += f'-te {left} {bottom} {right} {top} '
    command += '-r cubicspline '
    command += '-dstnodata -9999 '
    command += f'{vrt_filepath} {vrt_3857_filepath}'
    out, err = run_command(command)
    if err.strip() != '' and FAIL_ON_WARNING:
        raise Exception(f'gdalwarp failed for {vrt_filepath}:\n{out}\n{err}')

def translate(in_filepath, out_filepath):
    command = 'GDAL_CACHEMAX=64 GDAL_NUM_THREADS=1 gdal_translate --config GDAL_MAX_DATASET_POOL_SIZE 1 -of COG '
    command += '-co BIGTIFF=IF_NEEDED -co ADD_ALPHA=YES -co OVERVIEWS=NONE '
    command += '-co SPARSE_OK=YES -co BLOCKSIZE=512 -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 '
    command += f'{in_filepath} '
    command += f'{out_filepath}'
    out, err = run_command(command)
    if err.strip() != '' and FAIL_ON_WARNING:
        raise Exception(f'gdal_translate failed for {in_filepath}:\n{out}\n{err}')

def contains_nodata_pixels(filepath):
    with rasterio.env.Env(GDAL_CACHEMAX=64):
        with rasterio.open(filepath) as src:
            block_size = 1024
            for row in range(0, src.height, block_size):
                for col in range(0, src.width, block_size):
                    window = rasterio.windows.Window(
                        col_off=col,
                        row_off=row,
                        width=min(block_size, src.width - col),
                        height=min(block_size, src.height - row)
                    )
                    data = np.nan_to_num(src.read(1, window=window), nan=-9999)
                    if -9999 in data:
                        return True
    return False

# group source items by maxzoom and source
def get_grouped_source_items(filepath):
    lines = []
    with open(filepath) as f:
        lines = f.readlines()
    lines = lines[1:] # skip header
    line_tuples = []
    for line in lines:
        source, filename, maxzoom = line.strip().split(',')
        maxzoom = int(maxzoom)
        line_tuples.append((
            -maxzoom,
            source,
            filename
        ))
    line_tuples = sorted(line_tuples)
    grouped_source_items = []

    first_line_tuple = line_tuples[0]
    last_group_signature = (first_line_tuple[0], first_line_tuple[1])
    current_group = [{
        'maxzoom': -first_line_tuple[0],
        'source': first_line_tuple[1],
        'filename': first_line_tuple[2],
    }]
    for line_tuple in line_tuples[1:]:
        current_group_signature = (line_tuple[0], line_tuple[1])
        if current_group_signature != last_group_signature:
            grouped_source_items.append(current_group)
            current_group = []
            last_group_signature = current_group_signature
        current_group.append({
            'maxzoom': -line_tuple[0],
            'source': line_tuple[1],
            'filename': line_tuple[2],
        })
    grouped_source_items.append(current_group)
    return grouped_source_items

def reproject(filepath, tmp_folder):
    filename = filepath.split('/')[-1]

    z, x, y, _ = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]
    
    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)

    metadata_filepath = f'{tmp_folder}/reprojection.json'
    if os.path.isfile(metadata_filepath):
        print(f'reproject {filename} already done...')
        return

    grouped_source_items = get_grouped_source_items(filepath)
    maxzoom = grouped_source_items[0][0]['maxzoom']
    resolution = get_resolution(maxzoom)

    buffer_pixels = 0
    buffer_3857_rounded = 0
    if len(grouped_source_items) > 1:
        buffer_pixels = int(MACROTILE_BUFFER_3857 / resolution)
        buffer_3857_rounded = buffer_pixels * resolution

    for i, source_items in enumerate(grouped_source_items):
        vrt_filepath = create_virtual_raster(tmp_folder, i, source_items)
        zoom = maxzoom
        vrt_3857_filepath = f'{tmp_folder}/{i}-3857.vrt'
        create_warp(vrt_filepath, vrt_3857_filepath, zoom, aggregation_tile, buffer_3857_rounded)
        out_filepath = f'{tmp_folder}/{i}-3857.tiff'
        translate(vrt_3857_filepath, out_filepath)

        if len(grouped_source_items) > 1 and not contains_nodata_pixels(out_filepath):
            break
    
    metadata = {
        'buffer_pixels': buffer_pixels,
    }
    with open(metadata_filepath, 'w') as f:
        json.dump(metadata, f, indent=2)
