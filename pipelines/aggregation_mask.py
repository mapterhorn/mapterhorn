# Apply shoreline land/ocean mask to reprojected source groups.
import os
import tempfile

import numpy as np
import rasterio

import utils

NODATA = -9999
LAND_3857_GPKG = utils.store_dir('mask-store') + '/shoreline/land_3857.gpkg'
SHORELINE_VRT = utils.store_dir('mask-store') + '/shoreline/shoreline.vrt'


def get_source_domain(source):
    return utils.get_source_domain(source)


def load_shoreline_window(reference_src):
    # Rasterize land polygons into the exact grid of the reprojected source tile.
    if not os.path.isfile(LAND_3857_GPKG):
        raise FileNotFoundError(
            'Shoreline vectors missing at {}. Run source_prepare_shoreline.py first.'.format(LAND_3857_GPKG)
        )

    left, bottom, right, top = reference_src.bounds
    width = reference_src.width
    height = reference_src.height

    tmp_fd, tmp_path = tempfile.mkstemp(suffix='-shoreline.tif')
    os.close(tmp_fd)
    try:
        cmd = (
            'gdal_rasterize -q -burn 1 -init 0 -ot Byte '
            '-te {left} {bottom} {right} {top} '
            '-ts {width} {height} '
            '-of GTiff "{src}" "{dst}"'
        ).format(
            left=left, bottom=bottom, right=right, top=top,
            width=width, height=height,
            src=LAND_3857_GPKG, dst=tmp_path
        )
        out, err = utils.run_command(cmd, silent=True)
        if not os.path.isfile(tmp_path) or os.path.getsize(tmp_path) == 0:
            raise RuntimeError('gdal_rasterize failed for shoreline mask:\n{}\n{}'.format(out, err))
        with rasterio.open(tmp_path) as shore_src:
            mask = shore_src.read(1)
        if mask.shape != (height, width):
            raise RuntimeError('shoreline mask shape {} != expected {}'.format(mask.shape, (height, width)))
        return mask.astype(np.uint8)
    finally:
        if os.path.isfile(tmp_path):
            os.remove(tmp_path)


def apply_domain_mask(elevation, land_mask, domain):
    # land_mask: 1 = land, 0 = ocean
    out = elevation.copy()
    if domain == 'both':
        return out
    if domain == 'ocean':
        out[land_mask == 1] = NODATA
        return out
    # land (default): drop ocean pixels that are sea-level fill ( >= 0 )
    ocean = land_mask == 0
    out[ocean & (out >= 0)] = NODATA
    return out


def mask_reprojected_groups(filepath, tmp_folder):
    # Kept for manual/debug use; aggregation_reproject applies masks inline.
    from glob import glob
    done_filepath = '{}/mask-done'.format(tmp_folder)
    if os.path.isfile(done_filepath):
        print('mask already done...')
        return

    grouped_source_items = utils.get_grouped_source_items(filepath)
    land_mask = None
    for i, source_items in enumerate(grouped_source_items):
        tiff_path = '{}/{}-3857.tiff'.format(tmp_folder, i)
        if not os.path.isfile(tiff_path):
            continue
        domain = get_source_domain(source_items[0]['source'])
        with rasterio.open(tiff_path) as src:
            if land_mask is None:
                land_mask = load_shoreline_window(src)
            data = np.nan_to_num(src.read(1), nan=NODATA)
            profile = src.profile.copy()
        masked = apply_domain_mask(data, land_mask, domain)
        profile.update(nodata=NODATA)
        with rasterio.open(tiff_path, 'w', **profile) as dst:
            dst.write(masked, 1)

    with open(done_filepath, 'w') as f:
        f.write('ok\n')


if __name__ == '__main__':
    pass
