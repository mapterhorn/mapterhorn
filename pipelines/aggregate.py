from glob import glob
import json
import os
import shutil
import time

import mercantile
import rioxarray
from scipy import ndimage
import numpy as np

import local_config
import utils


def fetch_tiff(source, collection_id, x, y, z):
    print(f'copy tiff from remote cogify store to local cogify store', source, collection_id, x, y, z)
    
    remote_file = f'{local_config.remote_cogify_store_path}/3857/{source}/{collection_id}/{z}-{x}-{y}.tiff'
    local_file = f'cogify-store/3857/{source}/{collection_id}/{z}-{x}-{y}.tiff'
    command = f'rsync -ahv {remote_file} {local_file}'
    utils.run_command(command)

def make_vrt(source, items, aggregation_id, x, y, z, part_name):
    filenames_docker = []
    for item in items:
        filenames_docker.append(f'/mapterhorn/pipelines/cogify-store/3857/{source}/{item["collection_id"]}/{item["z"]}-{item["x"]}-{item["y"]}.tiff')
    output_folder = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts'
    utils.create_folder(output_folder)
    output = f'{output_folder}/{part_name}.vrt'
    if os.path.isfile(output):
        os.remove(output)
    output_docker = f'/mapterhorn/pipelines/{output}'
    command = f'''docker run -it --user $(id -u):$(id -g) -v $PWD:/mapterhorn/pipelines/ ghcr.io/osgeo/gdal:alpine-small-3.11.0 \\
    gdalbuildvrt \\
    {output_docker} \\
    {' '.join(filenames_docker)}
    '''

    utils.run_command(command)

def upsample(src, dst, aggregation_id, x, y, z, zoom, buffer, use_lerc=False):
    tile = mercantile.Tile(x=x, y=y, z=z)
    bounds = mercantile.xy_bounds(tile)
    left = bounds.left - buffer
    bottom = bounds.bottom - buffer
    right = bounds.right + buffer
    top = bounds.top + buffer
    resolution = (bounds.right - bounds.left) / 2 ** (zoom - z) / 512
    compression = '-co COMPRESS=lzw \\'
    if use_lerc:
        compression = '''-co COMPRESS=lerc \\
    -co MAX_Z_ERROR=0.01 \\
    -co MAX_Z_ERROR_OVERVIEW=0.02 \\'''
    command = f'''docker run -it --user $(id -u):$(id -g) -v $PWD:/mapterhorn/pipelines/ ghcr.io/osgeo/gdal:alpine-small-3.11.0 \\
    gdalwarp \\
    -of COG \\
    -co BIGTIFF=IF_SAFER \\
    -co BLOCKSIZE=512 \\
    -co OVERVIEWS=NONE \\
    {compression}
    -co NUM_THREADS=ALL_CPUS \\
    -co SPARSE_OK=YES \\
    -wo NUM_THREADS=ALL_CPUS \\
    -r cubicspline \\
    -tr {resolution} {-resolution} \\
    -te {left} {bottom} {right} {top} \\
    -overwrite \\
    -multi \\
    /mapterhorn/pipelines/{src} \\
    /mapterhorn/pipelines/{dst}
    '''

    utils.run_command(command)

def sample(src, dst, aggregation_id, x, y, z, buffer, use_lerc=False):

    tile = mercantile.Tile(x=x, y=y, z=z)
    bounds = mercantile.xy_bounds(tile)
    left = bounds.left - buffer
    bottom = bounds.bottom - buffer
    right = bounds.right + buffer
    top = bounds.top + buffer

    compression = '-co COMPRESS=lzw \\'
    if use_lerc:
        compression = '''-co COMPRESS=lerc \\
    -co MAX_Z_ERROR=0.01 \\
    -co MAX_Z_ERROR_OVERVIEW=0.02 \\'''

    command = f'''docker run -it --user $(id -u):$(id -g) -v $PWD:/mapterhorn/pipelines/ ghcr.io/osgeo/gdal:alpine-small-3.11.0 \\
    gdal_translate \\
    -of COG \\
    --config GDAL_NUM_THREADS all_cpus \\
    -co BIGTIFF=IF_SAFER \\
    -co ADD_ALPHA=YES \\
    -co OVERVIEWS=IGNORE_EXISTING \\
    -co BLOCKSIZE=512 \\
    {compression}
    -co WARP_RESAMPLING=bilinear \\
    -co OVERVIEW_RESAMPLING=bilinear \\
    -co SPARSE_OK=YES \\
    -co TARGET_SRS=EPSG:3857 \\
    -co EXTENT={left},{bottom},{right},{top}, \\
    /mapterhorn/pipelines/{src} \\
    /mapterhorn/pipelines/{dst}
    '''
    utils.run_command(command)

def get_maxzoom_from_aggregation_item(aggregation_item):
    items_with_source = aggregation_item[-1]
    source = items_with_source['source']
    collection_ids = utils.get_collection_ids(source)
    return utils.get_maxzoom(source, collection_ids[-1])

def can_copy_directly(aggregation_tile, aggregation_item):
    if len(aggregation_item) != 1:
        return False
    cogify_items = aggregation_item[0]['items']
    if len(cogify_items) != 1:
        return False
    cogify_item = cogify_items[0]
    if cogify_item['x'] != aggregation_tile.x:
        return False
    if cogify_item['y'] != aggregation_tile.y:
        return False
    if cogify_item['z'] != aggregation_tile.z:
        return False
    return True

def copy_directly(aggregtion_tile, aggregation_item, aggregation_id):
    source = aggregation_item[0]['source']
    cogify_item = aggregation_item[0]['items'][0]
    fetch_tiff(source, cogify_item['collection_id'], cogify_item['x'], cogify_item['y'], cogify_item['z'])
    src = f'cogify-store/3857/{source}/{cogify_item["collection_id"]}/{cogify_item["z"]}-{cogify_item["x"]}-{cogify_item["y"]}.tiff'
    dst = f'aggregation-store/{aggregation_id}/{aggregation_tile.z}-{aggregation_tile.x}-{aggregation_tile.y}.tiff'
    shutil.copyfile(src, dst)

def update_coverage(aggregation_id):
    coverage = {
        'type': 'FeatureCollection',
        'features': []
    }
    for filepath in glob(f'aggregation-store/{aggregation_id}/*.tiff'):
        _, __, aggregation_item_name = filepath.replace('.tiff', '').split('/')
        z, x, y = [int(a) for a in aggregation_item_name.split('-')]
        aggregation_tile = mercantile.Tile(x=x, y=y, z=z)
        coverage['features'].append(mercantile.feature(aggregation_tile))
    with open('tmp/coverage.geojson', 'w') as f:
        json.dump(coverage, f, indent=2)

# prepare local cogify store
remote_cogify_store = f'{local_config.remote_cogify_store_path}/3857/'
local_cogify_store = f'cogify-store/3857/'

utils.create_folder(local_cogify_store)
utils.rsync(src=remote_cogify_store, dst=local_cogify_store, skip_tiffs=True)

aggregation_ids = utils.get_aggregation_ids()
aggregation_id = aggregation_ids[-1]

aggregation_item_paths = list(sorted(glob(f'aggregation-store/{aggregation_id}/*-*-*.json')))
j = 0
t0 = time.time()
for aggregation_item_path in aggregation_item_paths:
    print(f'working on file {j} / {len(aggregation_item_paths)}. Total time so far {time.time() - t0} s.')
    tic = time.time()
    j += 1

    _, __, aggregation_item_name = aggregation_item_path.replace('.json', '').split('/')
    z, x, y = aggregation_item_name.split('-')
    z = int(z)
    x = int(x)
    y = int(y)

    aggregation_tile_filename = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}.tiff'
    aggregation_tile = mercantile.Tile(x=x, y=y, z=z)

    if os.path.isfile(aggregation_tile_filename):
        print(f'aggregation tile x={x} y={y} z={z} exists already...')
        continue

    # if x != 2131 or y != 1432:
    #     continue
    # if x != 8520 or y != 5836:
    #     continue

    aggregation_item = []
    with open(aggregation_item_path) as f:
        aggregation_item = json.load(f)

    if can_copy_directly(aggregation_tile, aggregation_item):
        print(f'can copy aggregation tile {aggregation_tile} directly...')
        # copy_directly(aggregation_tile, aggregation_item, aggregation_id)
        continue

    zoom = get_maxzoom_from_aggregation_item(aggregation_item)
    da_merged = None

    for items_with_source in reversed(aggregation_item):
        if da_merged is not None and da_merged.rio.nodata not in da_merged:
            break

        source = items_with_source['source']
        items = items_with_source['items']
        for item in items:
            fetch_tiff(source, item['collection_id'], item['x'], item['y'], item['z'])
        
        part_name = f'part-{source}'
        make_vrt(source, items, aggregation_id, x, y, z, part_name)

        if len(aggregation_item) == 1:
            break

        current_zoom = utils.get_maxzoom(source, items[0]['collection_id'])
        src = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/{part_name}.vrt'
        dst = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/{part_name}.tiff'
        if current_zoom == zoom:
            sample(src, dst, aggregation_id, x, y, z, local_config.macrotile_buffer_m)
        else:
            upsample(src, dst, aggregation_id, x, y, z, zoom, local_config.macrotile_buffer_m)

        da_current = rioxarray.open_rasterio(f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/{part_name}.tiff')

        if da_merged is None:
            da_merged = da_current.copy()
            continue
        
        t1 = time.time()
        binary_mask = (da_merged.values[0] != da_merged.rio.nodata).astype('int32')
        print(f'binary_mask done in {time.time() - t1} s...')

        bounds = mercantile.xy_bounds(mercantile.Tile(x=x, y=y, z=z))
        resolution = (bounds.right - bounds.left) / 2 ** (zoom - z) / 512
        max_pixel_distance = int(0.5 * local_config.macrotile_buffer_m / resolution)
        print('max_pixel_distance', max_pixel_distance)

        t1 = time.time()
        reduced = ndimage.binary_erosion(binary_mask, iterations=max_pixel_distance)
        print(f'binary erosion done in {time.time() - t1} s...')

        # t1 = time.time()
        # eroded = ndimage.binary_erosion(reduced)
        # edges = reduced ^ eroded
        # print(f'edges done in {time.time() - t1} s...')

        t1 = time.time()
        # distance_to_edge = ndimage.distance_transform_edt(1 - edges)
        # distance_to_edge = np.where(distance_to_edge > max_pixel_distance, max_pixel_distance, distance_to_edge)
        # distance_to_edge = np.where((1 - reduced), 0.0, distance_to_edge)
        # distance_to_edge = ndimage.uniform_filter(distance_to_edge, max_pixel_distance, mode='nearest')
        # distance_to_edge = np.where((1 - binary_mask), 0.0, distance_to_edge)
        # print(f'distance_to_edge done in {time.time() - t1} s...')
        # alpha_mask = distance_to_edge / max_pixel_distance

        t1 = time.time()

        alpha_mask = ndimage.uniform_filter(reduced.astype('float32'), int(1.25 * max_pixel_distance), mode='nearest')
        alpha_mask = 3 * alpha_mask ** 2 - 2 * alpha_mask ** 3 # smoothstep with zero derivative at 0 and 1
        alpha_mask = np.where((1 - binary_mask), 0.0, alpha_mask)
        print(f'alpha_mask done in {time.time() - t1} s...')

        # da_out = da_merged.copy()
        # da_out.values[0] = alpha_mask
        # da_out.rio.to_raster('alpha_mask2.tiff')
        # exit()

        t1 = time.time()
        merged = da_current.values[0] * (1 - alpha_mask) + da_merged.values[0] * alpha_mask
        da_merged.values[0] = merged
        print(f'merging done in {time.time() - t1} s...')

    input_filename = ''
    if len(aggregation_item) > 1:
        merged_filename = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/merged.tiff'
        da_merged.rio.to_raster(merged_filename)
        input_filename = merged_filename
    else:
        input_filename = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/part-{aggregation_item[0]["source"]}.vrt'
    sample(input_filename, aggregation_tile_filename, aggregation_id, x, y, z, 0, use_lerc=True)
    shutil.rmtree(f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/')

    update_coverage(aggregation_id)
    print(f'aggregation tile completed in {time.time() - tic} s')
