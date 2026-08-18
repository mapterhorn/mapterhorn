import subprocess
from pathlib import Path
from glob import glob
import math
import os
import hashlib

import numpy as np

import json

from rasterio.warp import transform_bounds
import mercantile
import imagecodecs
from pmtiles.tile import zxy_to_tileid, tileid_to_zxy, TileType, Compression
from pmtiles.writer import Writer

macrotile_z = 12
macrotile_buffer_3857 = 150
num_overviews = 6

X_MIN_3857, _, X_MAX_3857, __ = transform_bounds('EPSG:4326', 'EPSG:3857', -180, 0, 180, 0)
# Web Mercator is only valid to ~+-85.05112878 degrees latitude
Y_MIN_3857 = -X_MAX_3857
Y_MAX_3857 = X_MAX_3857

_SOURCE_DOMAIN_CACHE = {}

# ---------------------------------------------------------------------------
# Data stores live under MAPTERHORN_DATA_ROOT (default: pipelines/ cwd).
# Optional per-store overrides keep SSD/HDD split without symlinks in git.
# ---------------------------------------------------------------------------
STORE_NAMES = (
    'source-store',
    'aggregation-store',
    'tmp-store',
    'mask-store',
    'pmtiles-store',
    'bundle-store',
    'tar-store',
    'polygon-store',
    'meta-store',
    'task-store',
)

_STORE_ENV_VARS = {
    'source-store': 'MAPTERHORN_SOURCE_STORE',
    'aggregation-store': 'MAPTERHORN_AGGREGATION_STORE',
    'tmp-store': 'MAPTERHORN_TMP_STORE',
    'mask-store': 'MAPTERHORN_MASK_STORE',
    'pmtiles-store': 'MAPTERHORN_PMTILES_STORE',
    'bundle-store': 'MAPTERHORN_BUNDLE_STORE',
    'tar-store': 'MAPTERHORN_TAR_STORE',
    'polygon-store': 'MAPTERHORN_POLYGON_STORE',
    'meta-store': 'MAPTERHORN_META_STORE',
    'task-store': 'MAPTERHORN_TASK_STORE',
}

_PIPELINES_DIR = Path(__file__).resolve().parent


def data_root():
    root = os.environ.get('MAPTERHORN_DATA_ROOT')
    if root:
        return str(Path(root).expanduser().resolve())
    # Default: current working directory (normally pipelines/)
    return os.path.abspath(os.getcwd())


def catalog_root():
    override = os.environ.get('MAPTERHORN_CATALOG_ROOT')
    if override:
        return str(Path(override).expanduser().resolve())
    return str((_PIPELINES_DIR.parent / 'source-catalog').resolve())


def store_dir(name, create=True):
    if name not in _STORE_ENV_VARS:
        raise ValueError('unknown store {!r}'.format(name))
    env_key = _STORE_ENV_VARS[name]
    override = os.environ.get(env_key)
    if override:
        path = str(Path(override).expanduser().resolve())
    else:
        path = os.path.join(data_root(), name)
    if create:
        Path(path).mkdir(parents=True, exist_ok=True)
    return path


def store_path(name, *parts, create=True):
    base = store_dir(name, create=create)
    if not parts:
        return base
    # Allow store_path('source-store', 'a/b') as well as separate parts
    flat = []
    for part in parts:
        if part is None or part == '':
            continue
        flat.extend(str(part).replace('\\', '/').split('/'))
    return os.path.join(base, *flat)


def ensure_store_dirs():
    for name in STORE_NAMES:
        store_dir(name)


def catalog_path(*parts):
    return os.path.join(catalog_root(), *parts)
def run_command(command, silent=True, env=None, stream=False):
    if env is None:
        env = os.environ.copy()
    if not silent:
        print(command)
    if stream:
        # Inherit stdout/stderr so tools like wget can show a live progress bar
        p = subprocess.Popen(command, shell=True, env=env)
        p.communicate()
        if p.returncode != 0:
            return '', 'command failed with exit code {}'.format(p.returncode)
        return '', ''
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    stdout, stderr = p.communicate()
    err = stderr.decode()
    if err != '' and not silent:
        print(err)
    out = stdout.decode()
    if out != '' and not silent:
        print(out)
    return out, err

def wget_download(url, dest=None, cwd=None):
    # Live progress bar; --continue resumes partial downloads.
    # Raises if wget fails so callers never mark a download complete.
    parts = ['wget', '--continue', '--progress=bar:force']
    if dest is not None:
        parts.extend(['-O', '"{}"'.format(dest)])
    parts.append('"{}"'.format(url))
    command = ' '.join(parts)
    if cwd:
        command = 'cd {} && {}'.format(cwd, command)
    out, err = run_command(command, silent=False, stream=True)
    if err:
        raise RuntimeError('wget failed for {}: {}'.format(url, err))
    return out, err

def create_folder(path):
    folder_path = Path(path)
    folder_path.mkdir(parents=True, exist_ok=True)

def get_aggregation_ids():
    '''
    returns aggregation ids ordered from oldest to newest
    '''
    pattern = store_path('aggregation-store', '*')
    return list(sorted([path.split('/')[-1] for path in glob(pattern) if os.path.isdir(path)]))


def get_vertical_rounding_multiplier(z):
    return int(2 ** ((10 - z) / 2) / (1 / 256))


def save_terrarium_tile(data, filepath):
    filename = filepath.split('/')[-1]
    z = int(filename.split('-')[0])

    # full terrarium resolution of 1/256 at `full_resolution_zoom`
    # multiples of 2 of full terrarium resolution at lower zooms
    full_resolution_zoom = 19
    factor = 2 ** (full_resolution_zoom - z) / 256 
    data = np.round(data / factor) * factor

    data += 32768
    rgb = np.zeros((512, 512, 3), dtype=np.uint8)
    np.seterr(all='raise')
    try:
        rgb[..., 0] = data // 256
        rgb[..., 1] = data % 256
        rgb[..., 2] = (data - np.floor(data)) * 256
    except FloatingPointError:
        print('FloatingPointError raised in {}'.format(filepath))
        raise FloatingPointError()
    with open(filepath, 'wb') as f:
        f.write(imagecodecs.webp_encode(rgb, lossless=True))


def create_archive(tmp_folder, out_filepath):
    with open(out_filepath, 'wb') as f1:
        writer = Writer(f1)
        min_z = math.inf
        max_z = 0
        min_lon = math.inf
        min_lat = math.inf
        max_lon = -math.inf
        max_lat = -math.inf

        tile_ids = []
        for filepath in glob('{}/*.webp'.format(tmp_folder)):
            filename = filepath.split('/')[-1]
            z, x, y = [int(a) for a in filename.replace('.webp', '').split('-')]
            tile_ids.append(zxy_to_tileid(z=z, x=x, y=y))
        tile_ids = sorted(tile_ids)

        for tile_id in tile_ids:
            z, x, y = tileid_to_zxy(tile_id)
            filepath = '{}/{}-{}-{}.webp'.format(tmp_folder, z, x, y)
            with open(filepath, 'rb') as f2:
                writer.write_tile(tile_id, f2.read())

            max_z = max(max_z, z)
            min_z = min(min_z, z)
            west, south, east, north = mercantile.bounds(x, y, z)
            min_lon = min(min_lon, west)
            min_lat = min(min_lat, south)
            max_lon = max(max_lon, east)
            max_lat = max(max_lat, north)

        min_lon_e7 = int(min_lon * 1e7)
        min_lat_e7 = int(min_lat * 1e7)
        max_lon_e7 = int(max_lon * 1e7)
        max_lat_e7 = int(max_lat * 1e7)

        writer.finalize(
            {
                'tile_type': TileType.WEBP,
                'tile_compression': Compression.NONE,
                'min_zoom': min_z,
                'max_zoom': max_z,
                'min_lon_e7': min_lon_e7,
                'min_lat_e7': min_lat_e7,
                'max_lon_e7': max_lon_e7,
                'max_lat_e7': max_lat_e7,
                'center_zoom': int(0.5 * (min_z + max_z)),
                'center_lon_e7': int(0.5 * (min_lon_e7 + max_lon_e7)),
                'center_lat_e7': int(0.5 * (min_lat_e7 + max_lat_e7)),
            },
            {
                'attribution': '<a href="https://mapterhorn.com/attribution">© Mapterhorn</a>'
            },
        )


def get_aggregation_item_string(aggregation_id, filename):
    result = ''
    filepath = store_path('aggregation-store', aggregation_id, filename)
    if not os.path.isfile(filepath):
        return None
    
    with open(filepath) as f:
        result = ''.join([l.strip() for l in f.readlines()])
    
    return result.strip()


def get_dirty_aggregation_filenames(current_aggregation_id, last_aggregation_id):
    filepaths = sorted(glob(store_path('aggregation-store', current_aggregation_id, '*-aggregation.csv')))

    if last_aggregation_id is None:
        return [filepath.split('/')[-1] for filepath in filepaths]

    dirty_filenames = []
    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        current = get_aggregation_item_string(current_aggregation_id, filename)
        last = get_aggregation_item_string(last_aggregation_id, filename)
        if current != last:
            dirty_filenames.append(filename)
    return dirty_filenames


def get_pmtiles_folder(x, y, z):
    if z < 7:
        return store_dir('pmtiles-store')
    if z == 7:
        return store_path('pmtiles-store', '{}-{}-{}'.format(z, x, y))
    else:
        parent = mercantile.parent(mercantile.Tile(x=x, y=y, z=z), zoom=7)
        return store_path('pmtiles-store', '{}-{}-{}'.format(parent.z, parent.x, parent.y))


def get_source_domain(source):
    if source in _SOURCE_DOMAIN_CACHE:
        return _SOURCE_DOMAIN_CACHE[source]
    metadata_path = catalog_path(source, 'metadata.json')
    domain = 'land'
    if os.path.isfile(metadata_path):
        with open(metadata_path) as f:
            metadata = json.load(f)
        domain = metadata.get('domain', 'land')
    if domain not in ('land', 'ocean', 'both', 'mask'):
        raise ValueError('invalid domain {!r} for source {}'.format(domain, source))
    _SOURCE_DOMAIN_CACHE[source] = domain
    return domain


def clamp_bounds_3857(left, bottom, right, top):
    left = max(left, X_MIN_3857)
    right = min(right, X_MAX_3857)
    bottom = max(bottom, Y_MIN_3857)
    top = min(top, Y_MAX_3857)
    return left, bottom, right, top


# group source items by maxzoom and source
def get_grouped_source_items(filepath):
    lines = []
    with open(filepath) as f:
        lines = f.readlines()
    lines = lines[1:] # skip header
    line_tuples = []
    for line in lines:
        source, filename, maxzoom = line.strip().split(',')
        maxzoom = int(maxzoom)
        line_tuples.append((
            -maxzoom,
            source,
            filename
        ))
    line_tuples = sorted(line_tuples)
    grouped_source_items = []

    first_line_tuple = line_tuples[0]
    last_group_signature = (first_line_tuple[0], first_line_tuple[1])
    current_group = [{
        'maxzoom': -first_line_tuple[0],
        'source': first_line_tuple[1],
        'filename': first_line_tuple[2],
        'domain': get_source_domain(first_line_tuple[1]),
    }]
    for line_tuple in line_tuples[1:]:
        current_group_signature = (line_tuple[0], line_tuple[1])
        if current_group_signature != last_group_signature:
            grouped_source_items.append(current_group)
            current_group = []
            last_group_signature = current_group_signature
        current_group.append({
            'maxzoom': -line_tuple[0],
            'source': line_tuple[1],
            'filename': line_tuple[2],
            'domain': get_source_domain(line_tuple[1]),
        })
    grouped_source_items.append(current_group)
    return grouped_source_items

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