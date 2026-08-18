# Convert BathDNN25 NetCDF to sliced GeoTIFF tiles for aggregation.
from glob import glob
import os
import sys

import source_marker
import utils

def find_netcdf(source):
    matches = sorted(glob(utils.store_dir('source-store') + '/{}/*.nc'.format(source)))
    if not matches:
        raise FileNotFoundError('no .nc file in source-store/{}'.format(source))
    return matches[0]


def convert(source):
    nc_path = find_netcdf(source)
    out_dir = utils.store_dir('source-store') + '/{}'.format(source)
    utils.create_folder(out_dir)

    # Translate NetCDF to a single GeoTIFF (GDAL picks the elevation variable)
    global_tif = '{}/bathdnn25_global.tif'.format(out_dir)
    if not os.path.isfile(global_tif):
        print('translating {} -> {}'.format(nc_path, global_tif))
        cmd = (
            'gdal_translate -of GTiff -co TILED=YES -co COMPRESS=DEFLATE '
            '-co BIGTIFF=YES -a_nodata -9999 '
            '"{}" "{}"'
        ).format(nc_path, global_tif)
        out, err = utils.run_command(cmd, silent=False)
        if err and 'ERROR' in err.upper():
            # Try explicit subdataset discovery
            cmd = 'gdalinfo "{}"'.format(nc_path)
            info_out, _ = utils.run_command(cmd, silent=False)
            print(info_out)
            raise RuntimeError('gdal_translate failed for BathDNN NetCDF')

    # Slice into manageable tiles for polygonize
    print('slicing {}...'.format(global_tif))
    cmd = '{} source_slice.py {} 8192'.format(sys.executable, source)
    utils.run_command(cmd, silent=False)

    # Remove the oversized global mosaic if slices exist
    slices = [p for p in glob('{}/bathdnn25_global_*.tif'.format(out_dir)) if p != global_tif]
    # source_slice naming may differ — also accept any other .tif besides global
    other = [p for p in glob('{}/*.tif'.format(out_dir)) if os.path.basename(p) != 'bathdnn25_global.tif']
    if other and os.path.isfile(global_tif):
        print('removing global mosaic; {} tiles remain'.format(len(other)))
        os.remove(global_tif)


def main():
    if len(sys.argv) < 2:
        print('usage: source_bathdnn_convert.py <source>')
        exit(1)
    source = sys.argv[1]
    source_marker.require_download_complete(source)
    convert(source)


if __name__ == '__main__':
    main()
