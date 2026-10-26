# /// script
# requires-python = ">=3.13"
# dependencies = [
# ]
#
# [tool.pixi.workspace]
# channels = ["conda-forge"]
# ///

import sys
from glob import glob
import os

def main(source):
    filepaths = glob(f'source-store/{source}/*.tif')
    for filepath in filepaths:
        os.remove(filepath)

if __name__ == '__main__':
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'remove tifs of source {source}...')
    else:
        print('Not enough arguments. Usage: source_remove_tifs.py {{source}}')
        exit()
    main(source)
