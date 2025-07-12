import rasterio
import numpy as np
import os
import json

import local_config
import utils

def contains_nodata_pixels(filename_json):
    filename_tiff = filename_json.replace('.json', '.tiff')
    if not os.path.isfile(filename_tiff):
        return True
    with rasterio.open(filename_tiff) as src:
        data = src.read()
        return bool(np.any(data == src.nodata))

if __name__ == '__main__':
    # prepare local cogify store
    remote_cogify_store = f'{local_config.remote_cogify_store_path}/3857/{local_config.source}/'
    local_cogify_store = f'cogify-store/3857/{local_config.source}/'

    utils.create_folder(local_cogify_store)
    # utils.rsync(src=remote_cogify_store, dst=local_cogify_store)

    collection_ids = utils.get_collection_ids(local_config.source)
    collection_id = collection_ids[-1]
    items = utils.get_collection_items(local_config.source, collection_id)
    items.sort()

    j = 0
    for item in items:
        print(f'working on item {j + 1} / {len(items)}...')
        j += 1
        filename_json = f'cogify-store/3857/{local_config.source}/{collection_id}/{item}'
        key = 'mapterhorn:contains_nodata_pixels'
        feature = None
        with open(filename_json) as f:
            feature = json.load(f)
            if key in feature['properties']:
                continue
        feature['properties'][key] = contains_nodata_pixels(filename_json)
        print(f'{item} {"contains" if feature["properties"][key] else "does not contain"} nodata')
        with open(filename_json, 'w') as f:
            json.dump(feature, f, indent=2)
            
    # utils.rsync(src=local_cogify_store, dst=remote_cogify_store)

