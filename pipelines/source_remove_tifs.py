import sys
from glob import glob
import os

def main():
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'remove tifs of source {source}...')
    else:
        print('Not enough arguments. Usage: source_remove_tifs.py {{source}}')
        exit()

    filepaths = glob(f'source-store/{source}/*.tif')
    for filepath in filepaths:
        os.remove(filepath)

if __name__ == '__main__':
    main()
