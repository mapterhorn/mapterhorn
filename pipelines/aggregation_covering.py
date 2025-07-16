import os
import shutil
import json
from glob import glob

import mercantile
from ulid import ULID

import utils
import local_config

# todo: this function only looks at z-x-y.json files inside the collection_id folder, but some might be in older folders
# needs to be fixed
def get_cogify_items(source, collection_id):
    result = []
    filenames = glob(f'cogify-store/3857/{source}/{collection_id}/*-*-*.json')
    filenames.sort()

    for filename in filenames:
        data = None
        with open(filename) as f:
            data = json.load(f)
        x = data['properties']['linz_basemaps:options']['tile']['x']
        y = data['properties']['linz_basemaps:options']['tile']['y']
        z = data['properties']['linz_basemaps:options']['tile']['z']
        tile = mercantile.Tile(x=x, y=y, z=z)
        bounds = mercantile.xy_bounds(tile)
        contains_nodata_pixels = True
        if 'mapterhorn:contains_nodata_pixels' in data['properties']:
            contains_nodata_pixels = data['properties']['mapterhorn:contains_nodata_pixels']
        result.append({
            'tile': tile,
            'bounds': bounds,
            'has_overlap': False,
            'source': source,
            'collection_id': collection_id,
            'contains_nodata_pixels': contains_nodata_pixels,
            'hash': f'{source}/{collection_id}/{z}/{x}/{y}',
        })
    return result

def mark_cogify_items_for_overlap(cogify_items_by_source):
    sources = list(cogify_items_by_source.keys())
    for i in range(len(sources)):
        source = sources[i]
        for cogify_item in cogify_items_by_source[source]:
            tile = cogify_item['tile']
            for other_source in sources[(i+1):]:
                for other_cogify_item in cogify_items_by_source[other_source]:
                    other_tile = other_cogify_item['tile']
                    if utils.are_tiles_overlapping(tile, other_tile):
                        cogify_item['has_overlap'] = True
                        other_cogify_item['has_overlap'] = True

def write_overlap_free_items(aggregation_id, cogify_items_by_source):
    source = cogify_items_by_source.items()
    for source in sources:
        for cogify_item in cogify_items_by_source[source]:
            if not cogify_item['has_overlap']:
                aggregation_item = [{
                    'source': source,
                    'items': [{
                        'collection_id': cogify_item['collection_id'],
                        'x': cogify_item['tile'].x,
                        'y': cogify_item['tile'].y,
                        'z': cogify_item['tile'].z,
                        'contains_nodata_pixels': cogify_item['contains_nodata_pixels']
                    }]
                }]
                filename = f'aggregation-store/{aggregation_id}/{cogify_item["tile"].z}-{cogify_item["tile"].x}-{cogify_item["tile"].y}.json'
                with open(filename, 'w') as f:
                    json.dump(aggregation_item, f, indent=2)

def get_clean_cogify_items(cogify_items_by_source):
    clean_cogify_items = []
    for source in cogify_items_by_source.keys():
        for cogify_item in cogify_items_by_source[source]:
            if not cogify_item['has_overlap']:
                clean_cogify_items.append({
                    'source': source,
                    'collection_id': collection_id,
                    'x': cogify_item['tile'].x,
                    'y': cogify_item['tile'].y,
                    'z': cogify_item['tile'].z,
                })
    return clean_cogify_items

def get_intersecting_macrotiles(cogify_items):
    '''
    returns a list of unique mercantile.Tile s of the macrotiles that intersect the cogify items
    '''
    result = set({})
    for cogify_item in cogify_items:
        if not cogify_item['has_overlap']:
            continue
        tile = cogify_item['tile']
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

def serialize_clean_cogify_items_in_order(aggregation_id):
    tuples = []
    with open(f'aggregation-store/{aggregation_id}/clean_cogify_items.json') as f:
        clean_cogify_items = json.load(f)
        for tile in clean_cogify_items:
            tuples.append((
                tile['source'],
                tile['collection_id'],
                tile['x'],
                tile['y'],
                tile['z']
            ))
    return json.dumps([list(t) for t in sorted(tuples)])

def intersects(left, bottom, right, top, bounds):
    return not (
        right <= bounds.left or
        left >= bounds.right or
        top <= bounds.bottom or
        bottom >= bounds.top
    )

def group_cogify_items_by_source(cogify_items):
    result = {}
    for cogify_item in cogify_items:
        if cogify_item['source'] not in result:
            result[cogify_item['source']] = []
        result[cogify_item['source']].append(cogify_item)
    return result

def a_multi_neighbor_is_missing(macrotile, cogify_items_by_macrotile):
    neighbors = mercantile.neighbors(macrotile)
    for neighbor in neighbors:
        if neighbor not in cogify_items_by_macrotile:
            return True
        if len(cogify_items_by_macrotile[neighbor]) < 2:
            return True
    return False

def get_most_important_source(cogify_items, sources):
    present_sources = []
    for cogify_item in cogify_items:
        present_sources.append(cogify_item['source'])
    for source in reversed(sources):
        if source in present_sources:
            return source

def all_neighbors_share_most_important_source(macrotile, cogify_items_by_macrotile, sources):
    most_important_source = get_most_important_source(cogify_items_by_macrotile[macrotile], sources)
    neighbors = mercantile.neighbors(macrotile)
    for neighbor in neighbors:
        other_most_important_source = get_most_important_source(cogify_items_by_macrotile[neighbor], sources)
        if most_important_source != other_most_important_source:
            return False
    return True

def is_neighborhood_filled(macrotile, cogify_items_by_macrotile, sources):
    most_important_source = get_most_important_source(cogify_items_by_macrotile[macrotile], sources)
    neighbors = mercantile.neighbors(macrotile)
    neighbors.append(macrotile) # also check for self
    for neighbor in neighbors:
        neighbor_cogify_items = cogify_items_by_macrotile[neighbor]
        neighbor_cogify_items_by_source = group_cogify_items_by_source(neighbor_cogify_items)
        if len(neighbor_cogify_items_by_source[most_important_source]) != 1:
            return False
        cogify_item = neighbor_cogify_items_by_source[most_important_source][0]
        if cogify_item['contains_nodata_pixels']:
            return False
        if cogify_item['tile'].z < macrotile.z:
            return False
    return True

if __name__ == '__main__':
    aggregation_id = str(ULID())
    utils.create_folder(f'aggregation-store/{aggregation_id}')

    # prepare local cogify store
    remote_cogify_store = f'{local_config.remote_cogify_store_path}/3857/'
    local_cogify_store = f'cogify-store/3857/'

    utils.create_folder(local_cogify_store)
    utils.rsync(src=remote_cogify_store, dst=local_cogify_store, skip_tiffs=True)

    # prepare local aggregation store
    remote_aggregation_store = f'{local_config.remote_aggregation_store_path}/'
    local_aggregation_store = f'aggregation-store/'

    utils.create_folder(local_aggregation_store)
    # utils.rsync(src=remote_aggregation_store, dst=local_aggregation_store, skip_tiffs=True)

    sources = get_sources()
    cogify_items_by_source = {}
    collection_ids_by_source = {}

    print('get cogify items...')
    for source in sources:
        collection_ids = get_completed_collection_ids(source)
        collection_ids_by_source[source] = collection_ids
        cogify_items_by_source[source] = get_cogify_items(source, collection_ids[-1])

    print('mark cogify items for overlap with other cogify items...')
    mark_cogify_items_for_overlap(cogify_items_by_source)

    print('write aggregation items for cogify items without overlap...')
    write_overlap_free_items(aggregation_id, cogify_items_by_source)

    print('get cogify items by macrotile...')
    cogify_items_by_macrotile = {}
    for source in sources:
        for cogify_item in cogify_items_by_source[source]:
            if not cogify_item['has_overlap']:
                continue
            macrotiles = []
            if cogify_item['tile'].z < local_config.macrotile_z:
                macrotiles = mercantile.children(cogify_item['tile'], zoom=local_config.macrotile_z)
            elif cogify_item['tile'].z == local_config.macrotile_z:
                macrotiles = [cogify_item['tile']]
            else:
                macrotiles = [mercantile.parent(cogify_item['tile'], zoom=local_config.macrotile_z)]
            for macrotile in macrotiles:
                if macrotile not in cogify_items_by_macrotile:
                    cogify_items_by_macrotile[macrotile] = []
                if cogify_item['hash'] not in [other['hash'] for other in cogify_items_by_macrotile[macrotile]]:
                    cogify_items_by_macrotile[macrotile].append(cogify_item)

    print('find macrotiles that have only one cogify item...')      
    singles = []
    for macrotile, cogify_items in cogify_items_by_macrotile.items():
        if len(cogify_items) == 1:
            singles.append(macrotile)
    
    print('group singles by cogify item...')
    singles_by_cogify_item_hash = {}
    for macrotile in singles:
        cogify_item = cogify_items_by_macrotile[macrotile][0]
        if cogify_item['hash'] not in singles_by_cogify_item_hash:
            singles_by_cogify_item_hash[cogify_item['hash']] = []
        singles_by_cogify_item_hash[cogify_item['hash']].append(macrotile)

    singles_grouped = []
    for _, macrotile_group in singles_by_cogify_item_hash.items():
        singles_grouped.append(macrotile_group)
    
    print('simplify singles and write...')
    for group in singles_grouped:
        first_macrotile = group[0]
        first_cogify_item = cogify_items_by_macrotile[first_macrotile][0]
        for tile in mercantile.simplify(group):
            aggregation_item = [
                {
                    'source': first_cogify_item['source'],
                    'items': [{
                        'collection_id': first_cogify_item['collection_id'],
                        'x': first_cogify_item['tile'].x,
                        'y': first_cogify_item['tile'].y,
                        'z': first_cogify_item['tile'].z,
                        'contains_nodata_pixels': first_cogify_item['contains_nodata_pixels']
                    }]
                }
            ]
            filename = f'aggregation-store/{aggregation_id}/{tile.z}-{tile.x}-{tile.y}.json'
            with open(filename, 'w') as f:
                json.dump(aggregation_item, f, indent=2)

    finished_macrotiles = list(singles)

    print('find completely filled multis that are fully inside and write them...')
    for macrotile, cogify_items in cogify_items_by_macrotile.items():
        if len(cogify_items) < 2:
            continue
        if a_multi_neighbor_is_missing(macrotile, cogify_items_by_macrotile):
            continue
        if not all_neighbors_share_most_important_source(macrotile, cogify_items_by_macrotile, sources):
            continue
        if not is_neighborhood_filled(macrotile, cogify_items_by_macrotile, sources):
            continue

        cogify_items = cogify_items_by_macrotile[macrotile]
        cogify_items_by_source = group_cogify_items_by_source(cogify_items)
        most_important_source = get_most_important_source(cogify_items, sources)
        assert(len(cogify_items_by_source[most_important_source]) == 1)
        cogify_item = cogify_items_by_source[most_important_source][0]
        aggregation_item = [{
            'source': most_important_source,
            'items': [{
                'collection_id': cogify_item['collection_id'],
                'x': cogify_item['tile'].x,
                'y': cogify_item['tile'].y,
                'z': cogify_item['tile'].z,
                'contains_nodata_pixels': cogify_item['contains_nodata_pixels']
            }]
        }]
        with open(f'aggregation-store/{aggregation_id}/{macrotile.z}-{macrotile.x}-{macrotile.y}.json', 'w') as f:
            json.dump(aggregation_item, f, indent=2)
        finished_macrotiles.append(macrotile)

    print('write edge macrotiles...')
    for macrotile, cogify_items in cogify_items_by_macrotile.items():
        if macrotile in finished_macrotiles:
            continue
        
        all_cogify_items = list(cogify_items)
        neighbors = mercantile.neighbors(macrotile)
        for neighbor in neighbors:
            if neighbor in cogify_items_by_macrotile:
                all_cogify_items += cogify_items_by_macrotile[neighbor]
        
        unique_cogify_items = []
        hashes = []
        for cogify_item in all_cogify_items:
            if cogify_item['hash'] not in hashes:
                unique_cogify_items.append(cogify_item)
                hashes.append(cogify_item['hash'])
        
        cogify_items_by_source = group_cogify_items_by_source(unique_cogify_items)
        aggregation_item = []
        for source in sources:
            if source in cogify_items_by_source:
                items = []
                for cogify_item in cogify_items_by_source[source]:
                    items.append({
                        'collection_id': cogify_item['collection_id'],
                        'x': cogify_item['tile'].x,
                        'y': cogify_item['tile'].y,
                        'z': cogify_item['tile'].z,
                        'contains_nodata_pixels': cogify_item['contains_nodata_pixels'],
                    })
                aggregation_item.append({
                    'source': source,
                    'items': items,
                })
        with open(f'aggregation-store/{aggregation_id}/{macrotile.z}-{macrotile.x}-{macrotile.y}.json', 'w') as f:
            json.dump(aggregation_item, f, indent=2)
        finished_macrotiles.append(macrotile)

    # utils.rsync(src=local_aggregation_store, dst=remote_aggregation_store, skip_tiffs=True)
