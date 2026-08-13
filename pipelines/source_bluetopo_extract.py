# Extract the elevation band from multilayer NOAA BlueTopo GeoTIFFs.
from glob import glob
import os
import sys

import utils

def extract_band(source):
    folder = utils.store_dir('source-store') + '/{}'.format(source)
    filepaths = sorted(glob('{}/*.tiff'.format(folder)) + glob('{}/*.tif'.format(folder)))
    # Prefer original multilayer downloads named BlueTopo_*.tiff
    inputs = [p for p in filepaths if 'BlueTopo_' in os.path.basename(p) and '_elev' not in os.path.basename(p)]
    if not inputs:
        inputs = [p for p in filepaths if '_elev' not in os.path.basename(p)]

    print('extracting elevation band from {} BlueTopo files...'.format(len(inputs)))
    for i, filepath in enumerate(inputs):
        base = os.path.basename(filepath)
        name, ext = os.path.splitext(base)
        out_path = '{}/{}_elev.tif'.format(folder, name)
        if os.path.isfile(out_path):
            continue
        # Band 1 is elevation in BlueTopo multilayer products
        cmd = (
            'gdal_translate -b 1 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE '
            '-co BIGTIFF=IF_NEEDED "{}" "{}"'
        ).format(filepath, out_path)
        utils.run_command(cmd, silent=True)
        # Remove multilayer original to save space
        os.remove(filepath)
        if (i + 1) % 50 == 0:
            print('{} / {}'.format(i + 1, len(inputs)))
    print('done extracting BlueTopo elevation bands')


def main():
    if len(sys.argv) < 2:
        print('usage: source_bluetopo_extract.py <source>')
        exit(1)
    extract_band(sys.argv[1])


if __name__ == '__main__':
    main()
