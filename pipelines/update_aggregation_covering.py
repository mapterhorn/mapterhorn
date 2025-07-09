import os
import shutil
import json
from glob import glob

import mercantile
from shapely import Polygon
from ulid import ULID

import utils
import local_config

def get_covering_tiles(source, collection_id):
    result = [] # {polygon: shapely polygon in 3857, tile: mercantile Tile}
    data = {}
    with open(f'cogify-store/3857/{source}/{collection_id}/covering.geojson') as f:
        data = json.load(f)
    
    # feature['id'] example is '01JZ7Q7RCBK65E68ED6MVBSK65/8-132-92'
    tile_ids = [feature['id'].split('/')[-1] for feature in data['features']]
    
    for tile_id in tile_ids:
        z, x, y = [int(a) for a in tile_id.split('-')]
        tile = mercantile.Tile(x=x, y=y, z=z)
        feature = mercantile.feature(tile, projected='mercator')
        polygon = Polygon(feature['geometry']['coordinates'][0])
        result.append({
            'tile': tile,
            'polygon': polygon
        })

    return result

def get_intersecting_macrotiles(covering_tiles):
    result = set({})
    for covering_tile in covering_tiles:
        tile = covering_tile['tile']
        if tile.z < local_config.macrotile_z:
            result.update(mercantile.children(tile, zoom=local_config.macrotile_z))
        elif tile.z == local_config.macrotile_z:
            result.add(tile)
        else:
            result.add(mercantile.parent(tile, zoom=local_config.macrotile_z))
    return list(result)

def get_sources():
    '''
    returns a list of sources ordered by importance from low to high, 
    and the maxzoom of the most important source
    orders first by zoom from low to high, then by name anti-alphabetically
    lower maxzoom means less important
    source name later in alphabet means less important
    '''
    zoom_and_source = []
    sources = os.listdir('cogify-store/3857/')
    for source in sources:
        collection_ids = get_completed_collection_ids(source)
        if len(collection_ids) == 0:
            continue
        with open(f'cogify-store/3857/{source}/{collection_ids[-1]}/covering.geojson') as f:
            covering = json.load(f)
            zoom = utils.get_maxzoom(source, collection_ids[-1])
            zoom_and_source.append((-zoom, source))
    
    zoom_and_source = list(reversed(sorted(zoom_and_source)))
    return [s for _, s in zoom_and_source]

def is_cogify_done_on_collection(source, collection_id):
    filenames = glob(f'cogify-store/3857/{source}/{collection_id}/*-*-*.json')
    for filename in filenames:
        with open(filename) as f:
            item = json.load(f)
            if item['assets'] == {}:
                return False
    return True

def is_cogify_item_present(source, collection_id, x, y, z):
    return os.path.isfile(f'cogify-store/3857/{source}/{collection_id}/{z}-{x}-{y}.json')

def get_completed_collection_ids(source):
    result = []
    collection_ids = utils.get_collection_ids(source)
    for collection_id in collection_ids:
        if is_cogify_done_on_collection(source, collection_id):
            result.append(collection_id)
    return result

def to_tuple(source, tile):
    return (source, tile.x, tile.y, tile.z)

def from_tuple(t):
    return {
        'source': t[0],
        'x': t[1],
        'y': t[2],
        'z': t[3],
    }

def get_aggregation_item_string(aggregation_id, x, y, z):
    filename = f'aggregation-store/{aggregation_id}/{z}/{x}/{y}.json'
    if not os.path.isfile(filename):
        return None
    with open(filename) as f:
        return f.read()

def serialize_item_in_order(aggregation_id, x, y, z):
    filename = f'aggregation-store/{aggregation_id}/{z}/{x}/{y}.json'
    if not os.path.isfile(filename):
        return None
    entries = []
    with open(filename) as f:
        entries = json.load(f)
    
    result = []
    for entry in entries:
        lines = []
        for item in entry['items']:
            lines.append((entry['source'], item['collection_id'], item['x'], item['y'], item['z']))
        result.extend([list(l) for l in sorted(lines)])
    return json.dumps(result)

if __name__ == '__main__':
    # prepare local cogify store
    remote_cogify_store = f'{local_config.remote_cogify_store_path}/3857/'
    local_cogify_store = f'cogify-store/3857/'

    utils.create_folder(local_cogify_store)
    # utils.rsync(src=remote_cogify_store, dst=local_cogify_store, skip_tiffs=True)

    # prepare local aggregation store
    remote_aggregation_store = f'{local_config.remote_aggregation_store_path}/'
    local_aggregation_store = f'aggregation-store/'

    utils.create_folder(local_aggregation_store)
    # utils.rsync(src=remote_aggregation_store, dst=local_aggregation_store, skip_tiffs=True)

    sources = get_sources()
    covering_tiles_by_source = {}
    collection_ids_by_source = {}
    for source in sources:
        collection_ids = get_completed_collection_ids(source)
        collection_ids_by_source[source] = collection_ids
        covering_tiles_by_source[source] = get_covering_tiles(source, collection_ids[-1])

    macrotiles = set({})
    for source in sources:
        macrotiles.update(get_intersecting_macrotiles(covering_tiles_by_source[source]))
    
    macrotile_to_covering_tiles = {}
    for macrotile in macrotiles:
        macrotile_feature = mercantile.feature(macrotile, projected='mercator', buffer=local_config.macrotile_buffer)
        macrotile_poly = Polygon(macrotile_feature['geometry']['coordinates'][0])
        
        for source in sources:
            for covering_tile in covering_tiles_by_source[source]:
                if macrotile_poly.intersects(covering_tile['polygon']):
                    if macrotile in macrotile_to_covering_tiles:
                        macrotile_to_covering_tiles[macrotile].add(to_tuple(source, covering_tile['tile']))
                    else:
                        macrotile_to_covering_tiles[macrotile] = set([to_tuple(source, covering_tile['tile'])])

    aggregation_id = str(ULID())

    for macrotile in macrotiles:
        macrotile_metadata = {}
        for t in macrotile_to_covering_tiles[macrotile]:
            d = from_tuple(t)
            for collection_id in reversed(collection_ids_by_source[d['source']]):
                if is_cogify_item_present(d['source'], collection_id, d['x'], d['y'], d['z']):
                    if d['source'] not in macrotile_metadata:
                        macrotile_metadata[d['source']] = []
                    macrotile_metadata[d['source']].append({
                        'collection_id': collection_id,
                        'x': d['x'],
                        'y': d['y'],
                        'z': d['z'],
                    })
                    break
        macrotile_json = []
        for source in sources:
            if source in macrotile_metadata:
                macrotile_json.append({
                    'source': source,
                    'items': macrotile_metadata[source]
                })
        folder = f'aggregation-store/{aggregation_id}/{macrotile.z}/{macrotile.x}'
        utils.create_folder(folder)
        with open(f'{folder}/{macrotile.y}.json', 'w') as f:
            json.dump(macrotile_json, f, indent=2)
            
    aggregation_ids = utils.get_aggregation_ids()
    if len(aggregation_ids) > 1:
        new_aggregation_item_paths = glob(f'aggregation-store/{aggregation_id}/{local_config.macrotile_z}/**/*.json')
        for path in new_aggregation_item_paths:
            _, __, z, x, y = path.replace('.json', '').split('/')
            current_aggregation_item_string = serialize_item_in_order(aggregation_id, x, y, z)
            for previous_aggregation_id in reversed(aggregation_ids[:-1]):
                previous_aggregation_item_string = serialize_item_in_order(previous_aggregation_id, x, y, z)
                if previous_aggregation_item_string == current_aggregation_item_string:
                    os.remove(path)
                    break
        
        y_folders = glob(f'aggregation-store/{aggregation_id}/{local_config.macrotile_z}/*')
        for y_folder in y_folders:
            if glob(f'{y_folder}/*.json') == []:
                os.rmdir(y_folder)
        
        if glob(f'aggregation-store/{aggregation_id}/{local_config.macrotile_z}/*') == []:
            os.rmdir(f'aggregation-store/{aggregation_id}/{local_config.macrotile_z}')
            os.rmdir(f'aggregation-store/{aggregation_id}')

    # utils.rsync(src=local_aggregation_store, dst=remote_aggregation_store, skip_tiffs=True)
