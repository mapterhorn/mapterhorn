# Report where each pipeline store lives and how much free space it has.
# Run from pipelines/: uv run python storage_layout.py
from pathlib import Path
import os
import shutil

import utils


STORES = [
    ('source-store', 'SSD', 'random GeoTIFF reads'),
    ('aggregation-store', 'SSD', 'work CSVs / .todo .done'),
    ('tmp-store', 'SSD', 'hot scratch during aggregate'),
    ('mask-store', 'SSD preferred', 'shoreline vectors'),
    ('pmtiles-store', 'HDD', 'single-zoom PMTiles'),
    ('bundle-store', 'HDD', 'planet + 6-x-y bundles'),
    ('tar-store', 'HDD', 'source tarballs'),
    ('polygon-store', 'either', 'coverage polygons'),
    ('meta-store', 'either', 'status / attribution / logs'),
]


def mount_for(path):
    best = '/'
    best_len = 1
    try:
        with open('/proc/mounts') as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2:
                    continue
                mnt = parts[1]
                if path.startswith(mnt) and len(mnt) >= best_len:
                    best = mnt
                    best_len = len(mnt)
    except OSError:
        pass
    return best


def free_gb(path):
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return None


def main():
    root = utils.data_root()
    print('MAPTERHORN_DATA_ROOT → {}'.format(root))
    print('catalog → {}'.format(utils.catalog_root()))
    print()
    print('{:<20} {:<14} {:>8}  {}'.format('STORE', 'WANT', 'FREE_GB', 'RESOLVES TO'))
    print('-' * 100)
    for name, want, why in STORES:
        path = utils.store_dir(name, create=False)
        exists = os.path.isdir(path)
        free = free_gb(path if exists else root)
        free_s = '{:.0f}'.format(free) if free is not None else '?'
        status = path if exists else path + ' (not created yet)'
        mnt = mount_for(path if exists else root)
        print('{:<20} {:<14} {:>8}  {}  [mount {}]'.format(name, want, free_s, status, mnt))
        print('{}'.format('').ljust(20) + '  # {}'.format(why))
        env_key = utils._STORE_ENV_VARS[name]
        if os.environ.get(env_key):
            print('{}'.format('').ljust(20) + '  override {}={}'.format(env_key, os.environ[env_key]))

    print()
    print('Env: MAPTERHORN_SOFTLINK_SOURCE={}  MAPTERHORN_MAX_TMP_SOURCE_SIZE={} GiB  MAPTERHORN_NUM_WORKERS={}'.format(
        os.environ.get('MAPTERHORN_SOFTLINK_SOURCE', '0'),
        os.environ.get('MAPTERHORN_MAX_TMP_SOURCE_SIZE', '100'),
        os.environ.get('MAPTERHORN_NUM_WORKERS', '32'),
    ))
    print('Tip: put all stores outside git with MAPTERHORN_DATA_ROOT=/mnt/ssd/mapterhorn')
    print('     and optionally override HDD stores via MAPTERHORN_PMTILES_STORE etc.')


if __name__ == '__main__':
    main()
