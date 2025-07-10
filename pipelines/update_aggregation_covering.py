import os
import shutil
import json
from glob import glob

import mercantile
from ulid import ULID

import utils
import local_config

def get_covering_tiles(source, collection_id):
    result = []
    data = {}
    with open(f'cogify-store/3857/{source}/{collection_id}/covering.geojson') as f:
        data = json.load(f)
    
    # feature['id'] example is '01JZ7Q7RCBK65E68ED6MVBSK65/8-132-92'
    tile_ids = [feature['id'].split('/')[-1] for feature in data['features']]
    
    for tile_id in tile_ids:
        z, x, y = [int(a) for a in tile_id.split('-')]
        tile = mercantile.Tile(x=x, y=y, z=z)
        bounds = mercantile.xy_bounds(tile)
        result.append({
            'tile': tile,
            'bounds': bounds,
            'dirty': False
        })

    return result

def mark_covering_tiles(covering_tiles_by_source):
    sources = list(covering_tiles_by_source.keys())
    for i in range(len(sources)):
        source = sources[i]
        for covering_tile in covering_tiles_by_source[source]:
            tile = covering_tile['tile']
            for other_source in sources[(i+1):]:
                for other_covering_tile in covering_tiles_by_source[other_source]:
                    other_tile = other_covering_tile['tile']
                    a = None
                    b = None
                    if tile.z > other_tile.z:
                        a = mercantile.parent(tile, zoom=other_tile.z)
                        b = other_tile
                    elif tile.z == other_tile.z:
                        a = tile
                        b = other_tile
                    else:
                        a = tile
                        b = mercantile.parent(other_tile, zoom=tile.z)
                    if a == b:
                        covering_tile['dirty'] = True
                        other_covering_tile['dirty'] = True

def get_intersecting_macrotiles(covering_tiles):
    result = set({})
    for covering_tile in covering_tiles:
        if not covering_tile['dirty']:
            continue
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
    print('sources', sources)
    for source in sources:
        collection_ids = get_completed_collection_ids(source)
        print('collection_ids', collection_ids)
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
            if 'linz_basemaps:generated' not in item['properties']:
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

def intersects(left, bottom, right, top, bounds):
    return not (
        right <= bounds.left or
        left >= bounds.right or
        top <= bounds.bottom or
        bottom >= bounds.top
    )

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

    print('get covering tiles...')
    for source in sources:
        collection_ids = get_completed_collection_ids(source)
        collection_ids_by_source[source] = collection_ids
        covering_tiles_by_source[source] = get_covering_tiles(source, collection_ids[-1])

    print('mark covering tiles...')
    mark_covering_tiles(covering_tiles_by_source)

    print('get macrotiles...')
    macrotiles = set({})
    for source in sources:
        macrotiles.update(get_intersecting_macrotiles(covering_tiles_by_source[source]))
    
    print('len(macrotiles)', len(macrotiles))
    macrotile_to_covering_tiles = {}
    j = 0
    for macrotile in macrotiles:
        if j % 100 == 0:
            print(f'loop 1 working on {j}/{len(macrotiles)}...')
        j += 1

        macrotile_bounds = mercantile.xy_bounds(macrotile)
        left = macrotile_bounds.left - local_config.macrotile_buffer_m
        bottom = macrotile_bounds.bottom - local_config.macrotile_buffer_m
        right = macrotile_bounds.right + local_config.macrotile_buffer_m
        top = macrotile_bounds.top + local_config.macrotile_buffer_m

        for source in sources:
            for covering_tile in covering_tiles_by_source[source]:
                if intersects(left, bottom, right, top, covering_tile['bounds']):
                    if macrotile in macrotile_to_covering_tiles:
                        macrotile_to_covering_tiles[macrotile].add(to_tuple(source, covering_tile['tile']))
                    else:
                        macrotile_to_covering_tiles[macrotile] = set([to_tuple(source, covering_tile['tile'])])

    aggregation_id = str(ULID())

    j = 0
    for macrotile in macrotiles:
        if j % 100 == 0:
            print(f'loop 2 working on {j}/{len(macrotiles)}...')
        j += 1

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
