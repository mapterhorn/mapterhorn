from glob import glob
import sys
import tarfile
import os
import json

import source_marker
import utils

def main():
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'creating tarball for {source}...')
    else:
        print('Not enough arguments. Usage: source_create_tarball.py {{source}}')
        exit()

    source_marker.require_download_complete(source)

    utils.create_folder(utils.store_dir('tar-store') + '/')
    checksum = None
    filepath = f'{utils.store_dir("tar-store")}/{source}.tar'
    with open(filepath, 'wb') as f:
        writer = utils.HashWriter(f)
        with tarfile.open(fileobj=writer, mode='w') as tar:
            tar.add(utils.catalog_path(source, 'LICENSE.pdf'), 'LICENSE.pdf')
            tar.add(utils.catalog_path(source, 'metadata.json'), 'metadata.json')
            tar.add(f'{utils.store_dir("source-store")}/{source}/bounds.csv', 'bounds.csv')
            tar.add(f'{utils.store_dir("polygon-store")}/{source}.gpkg', 'coverage.gpkg')
            filepaths = glob(f'{utils.store_dir("source-store")}/{source}/*.tif')
            for j, filepath in enumerate(filepaths, 1):
                if j % 1000 == 0:
                    print(f'{j:_} / {len(filepaths):_}')
                filename = filepath.split('/')[-1]
                tar.add(filepath, f'files/{filename}')
        checksum = writer.md5.hexdigest()

    filesize = os.path.getsize(filepath)
    utils.create_folder(utils.store_dir('meta-store') + '/tar/')
    with open(f'{utils.store_dir("meta-store")}/tar/{source}.json', 'w') as f:
        json.dump({
            'size': filesize,
            'md5sum': checksum,
        }, f, indent=2)
    source_marker.mark_ready(source)

if __name__ == '__main__':
    main()

