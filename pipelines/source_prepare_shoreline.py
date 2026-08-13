# Download and prepare the global ocean/land shoreline vectors.
# Uses S2Coast-2023 (CC-BY 4.0) plus GSHHG for Antarctica (S2Coast excludes it).
# Aggregation rasterizes these vectors per tile (a global 30 m raster would be terabytes).
from glob import glob
import os
import zipfile

import utils

S2COAST_URL = 'https://zenodo.org/api/records/17092775/files/S2Coast2023_ERSIShapeFile_vector.zip/content'
GSHHG_URL = 'https://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip'

RAW_DIR = utils.store_dir('mask-store') + '/shoreline/raw'
OUT_DIR = utils.store_dir('mask-store') + '/shoreline'
ANTARCTICA_LAT = -60.0
# Coarse overview for tooling / sanity checks (~3 km); not used for aggregation masking
OVERVIEW_PIXEL_SIZE_3857 = 3000.0


def download_file(url, dest):
    if os.path.isfile(dest) and os.path.getsize(dest) > 0:
        print('already have {}'.format(dest))
        return
    utils.create_folder(os.path.dirname(dest))
    print('downloading {}...'.format(url))
    utils.wget_download(url, dest=dest)


def unzip(zip_path, dest_dir):
    utils.create_folder(dest_dir)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(dest_dir)


def find_shapefile(root, patterns):
    for pattern in patterns:
        matches = sorted(glob('{}/**/{}'.format(root, pattern), recursive=True))
        if matches:
            return matches[0]
    return None


def prepare_vectors():
    utils.create_folder(RAW_DIR)
    s2_zip = '{}/s2coast.zip'.format(RAW_DIR)
    gshhg_zip = '{}/gshhg.zip'.format(RAW_DIR)
    download_file(S2COAST_URL, s2_zip)
    download_file(GSHHG_URL, gshhg_zip)

    s2_dir = '{}/s2coast'.format(RAW_DIR)
    gshhg_dir = '{}/gshhg'.format(RAW_DIR)
    if not os.path.isdir(s2_dir) or not find_shapefile(s2_dir, ['*.shp']):
        unzip(s2_zip, s2_dir)
    if not os.path.isdir(gshhg_dir) or not find_shapefile(gshhg_dir, ['GSHHS_f_L1.shp', 'GSHHS_h_L1.shp']):
        unzip(gshhg_zip, gshhg_dir)

    s2_shp = find_shapefile(s2_dir, [
        '*Polygon*.shp',
        '*polygon*.shp',
        '*POLY*.shp',
        '*.shp',
    ])
    if s2_shp is None:
        raise FileNotFoundError('Could not find S2Coast shapefile under {}'.format(s2_dir))

    gshhg_shp = find_shapefile(gshhg_dir, [
        'GSHHS_f_L1.shp',
        'GSHHS_h_L1.shp',
    ])
    if gshhg_shp is None:
        raise FileNotFoundError('Could not find GSHHG L1 shapefile under {}'.format(gshhg_dir))

    return s2_shp, gshhg_shp


def build_combined_land_gpkg(s2_shp, gshhg_shp, out_gpkg_4326, out_gpkg_3857):
    utils.create_folder(os.path.dirname(out_gpkg_4326))
    tmp_s2 = '{}/s2_land.gpkg'.format(OUT_DIR)
    tmp_ant = '{}/gshhg_antarctica.gpkg'.format(OUT_DIR)

    cmd = 'ogr2ogr -overwrite -t_srs EPSG:4326 -nln land {} "{}"'.format(tmp_s2, s2_shp)
    utils.run_command(cmd, silent=False)

    cmd = (
        'ogr2ogr -overwrite -t_srs EPSG:4326 -nln land '
        '-clipsrc -180 -90 180 {lat} '
        '{out} "{src}"'
    ).format(lat=ANTARCTICA_LAT, out=tmp_ant, src=gshhg_shp)
    utils.run_command(cmd, silent=False)

    if os.path.isfile(out_gpkg_4326):
        os.remove(out_gpkg_4326)
    cmd = 'ogr2ogr -f GPKG {} {}'.format(out_gpkg_4326, tmp_s2)
    utils.run_command(cmd, silent=False)
    cmd = 'ogr2ogr -update -append -nln land {} {}'.format(out_gpkg_4326, tmp_ant)
    utils.run_command(cmd, silent=False)

    cmd = 'ogr2ogr -overwrite -t_srs EPSG:3857 {} {}'.format(out_gpkg_3857, out_gpkg_4326)
    utils.run_command(cmd, silent=False)


def build_coarse_overview(land_3857_gpkg, out_tif, out_vrt):
    left = utils.X_MIN_3857
    bottom = utils.Y_MIN_3857
    right = utils.X_MAX_3857
    top = utils.Y_MAX_3857
    tmp_raw = '{}/shoreline_overview_raw.tif'.format(OUT_DIR)
    cmd = (
        'gdal_rasterize -burn 1 -init 0 -ot Byte '
        '-te {left} {bottom} {right} {top} '
        '-tr {ps} {ps} '
        '-co TILED=YES -co COMPRESS=DEFLATE -co BIGTIFF=YES '
        '{src} {dst}'
    ).format(
        left=left, bottom=bottom, right=right, top=top,
        ps=OVERVIEW_PIXEL_SIZE_3857, src=land_3857_gpkg, dst=tmp_raw
    )
    utils.run_command(cmd, silent=False)
    cmd = (
        'gdal_translate -of COG -co COMPRESS=DEFLATE -co BLOCKSIZE=512 '
        '-co BIGTIFF=YES -co OVERVIEWS=NONE {} {}'
    ).format(tmp_raw, out_tif)
    utils.run_command(cmd, silent=False)
    if os.path.isfile(tmp_raw):
        os.remove(tmp_raw)
    cmd = 'gdalbuildvrt -overwrite {} {}'.format(out_vrt, out_tif)
    utils.run_command(cmd, silent=False)


def main():
    utils.create_folder(OUT_DIR)
    land_4326 = '{}/land.gpkg'.format(OUT_DIR)
    land_3857 = '{}/land_3857.gpkg'.format(OUT_DIR)
    overview_tif = '{}/shoreline.tif'.format(OUT_DIR)
    overview_vrt = '{}/shoreline.vrt'.format(OUT_DIR)
    ready_marker = '{}/READY'.format(OUT_DIR)

    if os.path.isfile(ready_marker) and os.path.isfile(land_3857):
        print('shoreline mask already prepared at {}'.format(land_3857))
        return

    print('preparing shoreline vectors...')
    s2_shp, gshhg_shp = prepare_vectors()
    print('S2Coast:', s2_shp)
    print('GSHHG:', gshhg_shp)
    build_combined_land_gpkg(s2_shp, gshhg_shp, land_4326, land_3857)
    print('building coarse shoreline overview (~3 km)...')
    build_coarse_overview(land_3857, overview_tif, overview_vrt)
    with open(ready_marker, 'w') as f:
        f.write('land_3857={}\n'.format(land_3857))
    print('done: {}'.format(land_3857))


if __name__ == '__main__':
    main()
