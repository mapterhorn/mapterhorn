import shutil
from glob import glob

import utils

def main(filepaths):
    aggregation_id = utils.get_aggregation_ids()[-1]
    tmp_source_folder = f'aggregation-store/{aggregation_id}/tmp-sources'
    utils.create_folder(tmp_source_folder)

    source_filename_collection = set({})
    sources = set({})
    for filepath in filepaths:
        for source_items in utils.get_grouped_source_items(filepath):
            for source_item in source_items:
                source_filename_collection.add((source_item['source'], source_item['filename']))
                sources.add(source_item['source'])

    for source in sources:
        utils.create_folder(f'{tmp_source_folder}/{source}')
    
    removed_files = 0
    for existing_filepath in glob(f'{tmp_source_folder}/*/*'):
        _, __, ___, source, filename = existing_filepath.split('/')
        if (source, filename) not in source_filename_collection:
            utils.run_command(f'rm {existing_filepath}')
            removed_files += 1
        else:
            source_filename_collection.remove((source, filename))
    print(f'removed {removed_files} files...')

    j = 0
    for source, filename in source_filename_collection:
        if j % 100 == 0:
            print(f'copy {j + 1} / {len(source_filename_collection)}')
        j += 1
        shutil.copy2(f'source-store/{source}/{filename}', f'{tmp_source_folder}/{source}/{filename}')
