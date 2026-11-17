# /// script
# requires-python = ">=3.13"
# dependencies = [
# ]
#
# [tool.pixi.workspace]
# channels = ["conda-forge"]
# ///

from glob import glob
import sys
import tarfile
import os
import json
import hashlib

class HashWriter:
    def __init__(self, f):
        self.f = f
        self.md5 = hashlib.md5()
    def write(self, data):
        self.md5.update(data)
        return self.f.write(data)
    def tell(self):
        return self.f.tell()
    def flush(self):
        return self.f.flush()
    def close(self):
        return self.f.close()

def main(source):
    os.makedirs('tar-store/', exist_ok=True)
    checksum = None
    out_filepath = f'tar-store/{source}.tar'
    with open(out_filepath, 'wb') as f:
        writer = HashWriter(f)
        with tarfile.open(fileobj=writer, mode='w') as tar:
            tar.add(f'../source-catalog/{source}/LICENSE.pdf', 'LICENSE.pdf')
            tar.add(f'../source-catalog/{source}/metadata.json', 'metadata.json')
            tar.add(f'source-store/{source}/bounds.csv', 'bounds.csv')
            tar.add(f'polygon-store/{source}.gpkg', 'coverage.gpkg')
            in_filepaths = glob(f'source-store/{source}/*.tif')
            for j, in_filepath in enumerate(in_filepaths, 1):
                if j % 1000 == 0:
                    print(f'{j:_} / {len(in_filepaths):_}')
                in_filename = in_filepath.split('/')[-1]
                tar.add(in_filepath, f'files/{in_filename}')
        checksum = writer.md5.hexdigest()

    out_filesize = os.path.getsize(out_filepath)
    os.makedirs('meta-store/tar/', exist_ok=True)
    with open(f'meta-store/tar/{source}.json', 'w') as f:
        json.dump({
            'size': out_filesize,
            'md5sum': checksum,
        }, f, indent=2)

if __name__ == '__main__':
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'creating tarball for {source}...')
    else:
        print('Not enough arguments. Usage: source_create_tarball.py {{source}}')
        exit()
    main(source)
