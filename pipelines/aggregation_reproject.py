import json
import os

import rasterio
import mercantile
import numpy as np

import aggregation_mask
import utils

SILENT = True
FAIL_ON_WARNING = False

def create_virtual_raster(tmp_folder, i, source_items):
    source = source_items[0]['source']
    vrt_filepath = '{}/{}.vrt'.format(tmp_folder, i)
    input_file_list_path = '{}/{}-file-list.txt'.format(tmp_folder, i)
    with open(input_file_list_path, 'w') as f:
        for source_item in source_items:
            f.write(utils.store_dir('tmp-store') + '/source/{}/{}\n'.format(source, source_item['filename']))
    command = 'gdalbuildvrt -overwrite -input_file_list {} {}'.format(input_file_list_path, vrt_filepath)
    out, err = utils.run_command(command, silent=SILENT)

    if 'heterogeneous projection' in err:
        raise Exception('heterogenous projection found in {}'.format(tmp_folder))

    if not SILENT:
        print(out, err)
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
    command += '-tr {} {} '.format(resolution, resolution)
    command += '-te {} {} {} {} '.format(left, bottom, right, top)
    command += '-r cubicspline '
    command += '-dstnodata -9999 '
    command += '{} {}'.format(vrt_filepath, vrt_3857_filepath)
    out, err = utils.run_command(command, silent=SILENT)
    if err.strip() != '' and FAIL_ON_WARNING:
        raise Exception('gdalwarp failed for {}:\n{}\n{}'.format(vrt_filepath, out, err))

def translate(in_filepath, out_filepath):
    command = 'GDAL_CACHEMAX=64 GDAL_NUM_THREADS=1 gdal_translate --config GDAL_MAX_DATASET_POOL_SIZE 1 -of COG '
    command += '-co BIGTIFF=IF_NEEDED -co ADD_ALPHA=YES -co OVERVIEWS=NONE '
    command += '-co SPARSE_OK=YES -co BLOCKSIZE=512 -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 '
    command += '{} '.format(in_filepath)
    command += '{}'.format(out_filepath)
    out, err = utils.run_command(command, silent=SILENT)
    if err.strip() != '' and FAIL_ON_WARNING:
        raise Exception('gdal_translate failed for {}:\n{}\n{}'.format(in_filepath, out, err))

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

def reproject(filepath, tmp_folder):
    filename = filepath.split('/')[-1]

    z, x, y, _ = [int(a) for a in filename.replace('-aggregation.csv', '').split('-')]
    
    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)

    metadata_filepath = '{}/reprojection.json'.format(tmp_folder)
    if os.path.isfile(metadata_filepath):
        print('reproject {} already done...'.format(filename))
        return

    grouped_source_items = utils.get_grouped_source_items(filepath)
    maxzoom = grouped_source_items[0][0]['maxzoom']
    resolution = get_resolution(maxzoom)

    # Always allow buffer when multiple source groups may contribute after land/ocean masking
    buffer_pixels = 0
    buffer_3857_rounded = 0
    if len(grouped_source_items) > 1:
        buffer_pixels = int(utils.macrotile_buffer_3857 / resolution)
        buffer_3857_rounded = buffer_pixels * resolution

    land_mask = None
    for i, source_items in enumerate(grouped_source_items):
        vrt_filepath = create_virtual_raster(tmp_folder, i, source_items)
        zoom = maxzoom
        vrt_3857_filepath = '{}/{}-3857.vrt'.format(tmp_folder, i)
        create_warp(vrt_filepath, vrt_3857_filepath, zoom, aggregation_tile, buffer_3857_rounded)
        out_filepath = '{}/{}-3857.tiff'.format(tmp_folder, i)
        translate(vrt_3857_filepath, out_filepath)

        # Apply shoreline mask before deciding whether lower-priority sources are needed
        with rasterio.open(out_filepath) as src:
            if land_mask is None:
                land_mask = aggregation_mask.load_shoreline_window(src)
            data = np.nan_to_num(src.read(1), nan=-9999)
            profile = src.profile.copy()
        domain = source_items[0].get('domain', utils.get_source_domain(source_items[0]['source']))
        masked = aggregation_mask.apply_domain_mask(data, land_mask, domain)
        profile.update(nodata=-9999)
        with rasterio.open(out_filepath, 'w', **profile) as dst:
            dst.write(masked, 1)

        if not contains_nodata_pixels(out_filepath):
            break
    
    metadata = {
        'buffer_pixels': buffer_pixels,
    }
    with open(metadata_filepath, 'w') as f:
        json.dump(metadata, f, indent=2)

    with open('{}/mask-done'.format(tmp_folder), 'w') as f:
        f.write('ok\n')
