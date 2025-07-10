from itertools import islice

import local_config
import utils

def create_item(collection_id, item):
    command = f'''docker run -it --user $(id -u):$(id -g) -v $PWD:/mapterhorn/pipelines/ ghcr.io/linz/basemaps/cli:v8 cogify create \
        /mapterhorn/pipelines/cogify-store/3857/{local_config.source}/{collection_id}/{item}
    '''
    utils.run_command(command)

if __name__ == '__main__':
    # prepare local source store
    remote_source_store = f'{local_config.remote_source_store_path}/{local_config.source}/'
    local_source_store = f'source-store/{local_config.source}/'

    utils.create_folder(local_source_store)
    utils.rsync(src=remote_source_store, dst=local_source_store)

    # prepare local cogify store
    remote_cogify_store = f'{local_config.remote_cogify_store_path}/3857/{local_config.source}/'
    local_cogify_store = f'cogify-store/3857/{local_config.source}/'

    utils.create_folder(local_cogify_store)
    utils.rsync(src=remote_cogify_store, dst=local_cogify_store)

    collection_ids = utils.get_collection_ids(local_config.source)
    items = utils.get_collection_items(local_config.source, collection_ids[-1])

    j = 0
    for item in items[local_config.machine_i::local_config.machine_count]:
        print(f'working on item {j}...')
        j += 1
        create_item(collection_ids[-1], item)

    utils.rsync(src=local_cogify_store, dst=remote_cogify_store)

