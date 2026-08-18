# Preflight checks before a long unattended run.
from glob import glob
import os
import shutil
import sys

import utils
import source_marker

MIN_FREE_GB = float(os.environ.get('MAPTERHORN_MIN_FREE_GB', '50'))


def check(name, ok, detail=''):
    status = 'OK' if ok else 'FAIL'
    print('[{}] {}{}'.format(status, name, (' — ' + detail) if detail else ''))
    return ok


def has_command(cmd):
    out, _ = utils.run_command('command -v {}'.format(cmd))
    return out.strip() != ''


def main():
    ok = True
    ok &= check('gdalwarp', has_command('gdalwarp'))
    ok &= check('gdal_translate', has_command('gdal_translate'))
    ok &= check('gdalbuildvrt', has_command('gdalbuildvrt'))
    ok &= check('gdal_rasterize', has_command('gdal_rasterize'))
    ok &= check('ogr2ogr', has_command('ogr2ogr'))
    ok &= check('wget', has_command('wget'))
    ok &= check('uv', has_command('uv'))

    data_root = utils.data_root()
    data_free = shutil.disk_usage(data_root).free / (1024 ** 3)
    ok &= check(
        'disk free (DATA_ROOT)',
        data_free >= MIN_FREE_GB,
        '{:.1f} GB at {} (min {})'.format(data_free, data_root, MIN_FREE_GB),
    )

    for name in ('source-store', 'aggregation-store', 'tmp-store', 'pmtiles-store'):
        real = utils.store_dir(name, create=False)
        try:
            target = real if os.path.isdir(real) else data_root
            store_free = shutil.disk_usage(target).free / (1024 ** 3)
            check(
                'disk free {}'.format(name),
                store_free >= MIN_FREE_GB,
                '{:.1f} GB free → {}'.format(store_free, real),
            )
        except OSError as e:
            ok &= check('disk free {}'.format(name), False, str(e))

    shoreline = (
        os.path.isfile(utils.store_path('mask-store', 'shoreline', 'land_3857.gpkg', create=False))
        or os.path.isfile(utils.store_path('mask-store', 'shoreline', 'READY', create=False))
    )
    ok &= check(
        'shoreline mask',
        shoreline,
        utils.store_path('mask-store', 'shoreline', 'land_3857.gpkg', create=False),
    )

    land_bounds = []
    ocean_bounds = []
    incomplete = []
    for path in glob(utils.store_path('source-store', '*', 'bounds.csv', create=False)):
        source = path.split('/')[-2]
        if not source_marker.is_source_ready(source):
            incomplete.append(source)
            continue
        domain = utils.get_source_domain(source)
        if domain == 'land':
            land_bounds.append(source)
        elif domain == 'ocean':
            ocean_bounds.append(source)
        elif domain == 'both':
            land_bounds.append(source)
            ocean_bounds.append(source)

    if incomplete:
        print('[WARN] sources not READY (ignored): {}'.format(', '.join(incomplete)))

    ok &= check('land source bounds', len(land_bounds) > 0, ', '.join(land_bounds[:5]) or 'none')
    if len(ocean_bounds) == 0:
        print('[WARN] no ocean source bounds.csv found — oceans will stay at sea level')
    else:
        check('ocean source bounds', True, ', '.join(ocean_bounds[:8]))

    if not ok:
        print('\nPreflight failed.')
        sys.exit(1)
    print('\nPreflight passed.')


if __name__ == '__main__':
    main()
