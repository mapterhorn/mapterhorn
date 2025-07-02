from glob import glob
import json
from datetime import datetime
import os
import shutil

import utils
import local_config

def create_new_covering():
    command = f'''docker run -it --user $(id -u):$(id -g) -v $PWD:/mapterhorn/web-mercator-pipeline/ ghcr.io/linz/basemaps/cli:v8 cogify cover \
    --tile-matrix WebMercatorQuad \
    --preset lerc_10mm \
    --target /mapterhorn/web-mercator-pipeline/cogify-store/ \
    /mapterhorn/web-mercator-pipeline/source-store/{local_config.source}/
    '''

    utils.run_command(command)

def get_collection_ids():
    '''
    returns collection ids ordered from oldest to newest
    '''
    paths = glob(f'cogify-store/3857/{local_config.source}/*')
    collection_ids = [path.split('/')[-1] for path in paths]
    timestamps = []
    for collection_id in collection_ids:
        with open(f'cogify-store/3857/{local_config.source}/{collection_id}/collection.json') as f:
            collection = json.load(f)
            time_string = collection['extent']['temporal']['interval'][0][0]
            time_string = time_string.replace('Z', '+00:00')
            timestamps.append(datetime.fromisoformat(time_string))

    return [sorted_id for _, sorted_id in sorted(zip(timestamps, collection_ids))]

def get_collection_items(collection_id):
    paths = glob(f'cogify-store/3857/{local_config.source}/{collection_id}/*.json')
    filenames = [path.split('/')[-1] for path in paths]
    return [item for item in filenames if item not in ['collection.json', 'covering.json', 'source.json']]

def get_last_modification(collection_ids):

    items_by_collection_id = {}
    for collection_id in collection_ids:
        items_by_collection_id[collection_id] = get_collection_items(collection_id)

    latest_collection_id = collection_ids[-1]
    previous_collection_ids = collection_ids[:-1]

    last_modification = {}
    for item in items_by_collection_id[latest_collection_id]:
        last_modification[item] = None
        for previous_collection_id in previous_collection_ids[::-1]:
            if item in items_by_collection_id[previous_collection_id]:
                last_modification[item] = previous_collection_id
                break

    return last_modification

def get_source_hrefs(collection_id, item):
    data = {}
    with open(f'cogify-store/3857/{local_config.source}/{collection_id}/{item}') as f:
        data = json.load(f)
    return [link['href'] for link in data['links'] if link['rel'] == 'linz_basemaps:source']

if __name__ == '__main__':
    # prepare local source store
    remote_source_store = f'{local_config.remote_source_store_path}/{local_config.source}/'
    local_source_store = f'source-store/{local_config.source}/'

    utils.create_local_store(local_source_store)
    utils.rsync(src=remote_source_store, dst=local_source_store)

    # prepare local cogify store
    remote_cogify_store = f'{local_config.remote_cogify_store_path}/3857/{local_config.source}/'
    local_cogify_store = f'cogify-store/3857/{local_config.source}/'

    utils.create_local_store(local_cogify_store)
    utils.rsync(src=remote_cogify_store, dst=local_cogify_store)

    create_new_covering()

    # compute which items have changes
    changed_items = []
    collection_ids = get_collection_ids()
    last_modification = get_last_modification(collection_ids)
    for item in last_modification.keys():
        if last_modification[item] is None:
            changed_items.append(item)
        else:
            latest_source_hrefs = get_source_hrefs(collection_ids[-1], item)
            previous_source_hrefs = get_source_hrefs(last_modification[item], item)
            if set(latest_source_hrefs) != set(previous_source_hrefs):
                changed_items.append(item)
    
    if len(changed_items) == 0:
        # no items have changed, remove latest collection
        shutil.rmtree(f'cogify-store/3857/{local_config.source}/{collection_ids[-1]}')
    else:
        # some times have changed, remove the ones that did not
        for item in last_modification.keys():
            if item not in changed_items:
                os.remove(f'cogify-store/3857/{local_config.source}/{collection_ids[-1]}/{item}')
    
    utils.rsync(src=local_cogify_store, dst=remote_cogify_store)

    print(f'Number of items that have changed: {len(changed_items)}')
    if len(changed_items) > 0:
        print(f'New collection: {collection_ids[-1]}')
