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

def get_maxzoom_from_covering_tiles(covering_tiles):
    items_with_source = covering_tiles[-1]
    source = items_with_source['source']
    collection_ids = utils.get_collection_ids(source)
    return utils.get_maxzoom(source, collection_ids[-1])

def has_nodata(source, item):
    filename = f'cogify-store/3857/{source}/{item["collection_id"]}/{item["z"]}-{item["x"]}-{item["y"]}.tiff'
    da = rioxarray.open_rasterio(filename)
    return da.rio.nodata in da.values[0]

def filter_covering_tiles(macrotile, covering_tiles):
    result = []
    for items_with_source in covering_tiles:
        overlapping_items = []
        for item in items_with_source['items']:
            if utils.are_tiles_overlapping(macrotile, mercantile.Tile(x=item['x'], y=item['y'], z=item['z'])):
                overlapping_items.append(item)
        if len(overlapping_items) > 0:
            result.append(items_with_source)
    return result

# prepare local cogify store
remote_cogify_store = f'{local_config.remote_cogify_store_path}/3857/'
local_cogify_store = f'cogify-store/3857/'

utils.create_folder(local_cogify_store)
utils.rsync(src=remote_cogify_store, dst=local_cogify_store, skip_tiffs=True)

aggregation_ids = utils.get_aggregation_ids()
aggregation_id = aggregation_ids[-1]

z = local_config.macrotile_z
paths = list(sorted(glob(f'aggregation-store/{aggregation_id}/{z}-*-*.json')))
j = 0
t0 = time.time()
for path in paths:
    print(f'working on file {j} / {len(paths)}. Total time so far {time.time() - t0} s.')
    tic = time.time()
    j += 1

    _, __, item_name = path.replace('.json', '').split('/')
    _, x, y = item_name.split('-')
    x = int(x)
    y = int(y)

    macrotile_filename = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}.tiff'
    macrotile = mercantile.Tile(x=x, y=y, z=z)

    # if os.path.isfile(macrotile_filename):
    #     print(f'macrotile x={x} y={y} z={z} exists already...')
    #     continue

    if x != 2116 or y != 1450:
        continue
    # if x != 8520 or y != 5836:
    #     continue

    covering_tiles = []
    with open(path) as f:
        covering_tiles = json.load(f)

    covering_tiles = filter_covering_tiles(macrotile, covering_tiles)

    zoom = get_maxzoom_from_covering_tiles(covering_tiles)
    da_merged = None

    for items_with_source in reversed(covering_tiles):
        if da_merged is not None and da_merged.rio.nodata not in da_merged:
            break

        source = items_with_source['source']
        items = items_with_source['items']
        for item in items:
            fetch_tiff(source, item['collection_id'], item['x'], item['y'], item['z'])
        
        part_name = f'part-{source}'
        make_vrt(source, items, aggregation_id, x, y, local_config.macrotile_z, part_name)

        if len(covering_tiles) == 1:
            break

        current_zoom = utils.get_maxzoom(source, items[0]['collection_id'])
        src = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/{part_name}.vrt'
        dst = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/{part_name}.tiff'
        # if current_zoom == zoom:
        #     sample(src, dst, aggregation_id, x, y, z, local_config.macrotile_buffer_m)
        # else:
        #     upsample(src, dst, aggregation_id, x, y, z, zoom, local_config.macrotile_buffer_m)

        da_current = rioxarray.open_rasterio(f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/{part_name}.tiff')

        if da_merged is None:
            da_merged = da_current.copy()
            continue
        
        binary_mask = (da_merged.values[0] != da_merged.rio.nodata).astype('int32')
        print('binary_mask done...')

        bounds = mercantile.xy_bounds(mercantile.Tile(x=x, y=y, z=z))
        resolution = (bounds.right - bounds.left) / 2 ** (zoom - z) / 512
        max_pixel_distance = int(0.75*local_config.macrotile_buffer_m / resolution)
        print('max_pixel_distance', max_pixel_distance)
        reduced = ndimage.binary_erosion(binary_mask, iterations=max_pixel_distance)
        print('binary erosion done...')

        # eroded = ndimage.binary_erosion(reduced)
        # edges = reduced ^ eroded
        # print('edges done...')


        # distance_to_edge = ndimage.distance_transform_edt(1 - edges)
        # distance_to_edge = np.where(distance_to_edge > max_pixel_distance, max_pixel_distance, distance_to_edge)
        # distance_to_edge = np.where((1 - reduced), 0.0, distance_to_edge)
        # distance_to_edge = ndimage.uniform_filter(distance_to_edge, max_pixel_distance, mode='nearest')
        # distance_to_edge = np.where((1 - binary_mask), 0.0, distance_to_edge)
        # print('distance_to_edge done...')

        # alpha_mask = distance_to_edge / max_pixel_distance

        alpha_mask = ndimage.uniform_filter(reduced.astype('float32'), int(0.75 * max_pixel_distance), mode='nearest')
        alpha_mask = 3 * alpha_mask ** 2 - 2 * alpha_mask ** 3 # smoothstep with zero derivative at 0 and 1
        alpha_mask = np.where((1 - binary_mask), 0.0, alpha_mask)
        print('alpha_mask done...')

        # da_out = da_merged.copy()
        # da_out.values[0] = alpha_mask
        # da_out.rio.to_raster('alpha_mask2.tiff')
        # exit()

        merged = da_current.values[0] * (1 - alpha_mask) + da_merged.values[0] * alpha_mask
        da_merged.values[0] = merged
        print('merging done...')

    input_filename = ''
    if len(covering_tiles) > 1:
        merged_filename = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/merged.tiff'
        da_merged.rio.to_raster(merged_filename)
        input_filename = merged_filename
    else:
        input_filename = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/part-{covering_tiles[0]["source"]}.vrt'
    macrotile_filename = f'aggregation-store/{aggregation_id}/{z}-{x}-{y}.tiff'
    sample(input_filename, macrotile_filename, aggregation_id, x, y, z, 0, use_lerc=True)
    # shutil.rmtree(f'aggregation-store/{aggregation_id}/{z}-{x}-{y}-parts/')

    print(f'macrotile completed in {time.time() - tic} s')