# Write and report pipeline run status for unattended operation.
from datetime import datetime, timezone
from glob import glob
import json
import os
import shutil
import sys

import utils
import source_marker

STATUS_PATH = utils.store_dir('meta-store') + '/run-status.json'


def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def load_status():
    if not os.path.isfile(STATUS_PATH):
        return {
            'updated_at': None,
            'stage': None,
            'run_id': os.environ.get('MAPTERHORN_RUN_ID'),
            'aggregation': {},
            'downsampling': {},
            'bundle': {},
            'sources': {},
            'disk': {},
            'last_error': None,
            'failures': [],
        }
    with open(STATUS_PATH) as f:
        return json.load(f)


def save_status(data):
    utils.create_folder(utils.store_dir('meta-store'))
    data['updated_at'] = utc_now()
    tmp = STATUS_PATH + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, STATUS_PATH)


def disk_free_gb(path):
    if not os.path.isdir(path):
        return None
    usage = shutil.disk_usage(path)
    return round(usage.free / (1024 ** 3), 2)


def count_suffix(aggregation_id, kind, suffix):
    return len(glob(utils.store_dir('aggregation-store') + '/{}/*-{}.csv{}'.format(aggregation_id, kind, suffix)))


def count_children(aggregation_id, kind, suffix):
    total = 0
    for filepath in glob(utils.store_dir('aggregation-store') + '/{}/*-{}.csv{}'.format(aggregation_id, kind, suffix)):
        filename = filepath.split('/')[-1]
        stem = filename.replace('-{}.csv{}'.format(kind, suffix), '')
        parts = stem.split('-')
        if len(parts) < 4:
            continue
        z, x, y, child_z = [int(a) for a in parts[:4]]
        total += 2 ** (2 * (child_z - z))
    return total


def eta_from_progress(done, total, start_iso):
    if not start_iso or done <= 0 or total <= 0 or done >= total:
        return None
    start = datetime.fromisoformat(start_iso.replace('Z', '+00:00'))
    now = datetime.now(timezone.utc)
    elapsed = (now - start).total_seconds()
    if elapsed <= 0:
        return None
    remaining = elapsed * (total - done) / done
    eta = now.timestamp() + remaining
    return datetime.fromtimestamp(eta, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def source_download_progress():
    result = {}
    for bounds in glob(utils.store_dir('source-store') + '/*/bounds.csv'):
        source = bounds.split('/')[-2]
        tifs = glob(utils.store_dir('source-store') + '/{}/*.tif'.format(source)) + glob(utils.store_dir('source-store') + '/{}/*.tiff'.format(source))
        list_path = utils.catalog_path(source, 'file_list.txt')
        expected = 0
        if os.path.isfile(list_path):
            with open(list_path) as f:
                expected = sum(1 for line in f if line.strip() and not line.strip().startswith('#'))
        result[source] = {
            'tif_count': len(tifs),
            'expected_urls': expected,
            'has_bounds': True,
            'download_complete': source_marker.is_download_complete(source),
            'ready': source_marker.is_source_ready(source),
            'domain': utils.get_source_domain(source) if os.path.isdir(utils.catalog_path(source)) else None,
        }
    # Sources downloaded but without bounds yet
    for folder in glob(utils.store_dir('source-store') + '/*/'):
        source = folder.rstrip('/').split('/')[-1]
        if source in result:
            continue
        tifs = glob('{}/*.tif'.format(folder)) + glob('{}/*.tiff'.format(folder))
        result[source] = {
            'tif_count': len(tifs),
            'expected_urls': None,
            'has_bounds': False,
            'download_complete': source_marker.is_download_complete(source),
            'ready': source_marker.is_source_ready(source),
            'domain': utils.get_source_domain(source) if os.path.isdir(utils.catalog_path(source)) else None,
        }
    return result


def collect_failures(aggregation_id):
    failures = []
    for path in sorted(glob(utils.store_dir('aggregation-store') + '/{}/*.failed'.format(aggregation_id))):
        item = path.split('/')[-1]
        err = ''
        try:
            with open(path) as f:
                err = f.read().strip()[:500]
        except Exception:
            pass
        failures.append({'item': item, 'error': err})
    return failures


def refresh():
    data = load_status()
    data['disk'] = {
        'source_store_free_gb': disk_free_gb(utils.store_dir('source-store')),
        'aggregation_store_free_gb': disk_free_gb(utils.store_dir('aggregation-store')) if os.path.isdir(utils.store_dir('aggregation-store')) else disk_free_gb('.'),
        'pmtiles_store_free_gb': disk_free_gb(utils.store_dir('pmtiles-store')) if os.path.isdir(utils.store_dir('pmtiles-store')) else None,
    }
    data['sources'] = source_download_progress()
    data['shoreline_mask'] = os.path.isfile(utils.store_dir('mask-store') + '/shoreline/land_3857.gpkg')

    aggregation_ids = utils.get_aggregation_ids()
    if aggregation_ids:
        aggregation_id = aggregation_ids[-1]
        data['aggregation_id'] = aggregation_id
        for kind in ('aggregation', 'downsampling'):
            total = count_suffix(aggregation_id, kind, '')
            todo = count_suffix(aggregation_id, kind, '.todo')
            done = count_suffix(aggregation_id, kind, '.done')
            failed = count_suffix(aggregation_id, kind, '.failed')
            children_done = count_children(aggregation_id, kind, '.done')
            children_total = count_children(aggregation_id, kind, '')
            section = data.get(kind, {})
            if 'started_at' not in section and done > 0:
                # approximate from oldest .done mtime
                done_paths = glob(utils.store_dir('aggregation-store') + '/{}/*-{}.csv.done'.format(aggregation_id, kind))
                if done_paths:
                    oldest = min(os.path.getmtime(p) for p in done_paths)
                    section['started_at'] = datetime.fromtimestamp(oldest, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            section.update({
                'items_total': total,
                'items_todo': todo,
                'items_done': done,
                'items_failed': failed,
                'children_done': children_done,
                'children_total': children_total,
                'percent': round(100.0 * done / total, 2) if total else 0.0,
                'eta': eta_from_progress(done, total, section.get('started_at')),
            })
            data[kind] = section
        data['failures'] = collect_failures(aggregation_id)
    save_status(data)
    return data


def heartbeat(stage, **fields):
    data = load_status()
    data['stage'] = stage
    data['heartbeat_at'] = utc_now()
    data['run_id'] = os.environ.get('MAPTERHORN_RUN_ID', data.get('run_id'))
    for key, value in fields.items():
        if key in ('aggregation', 'downsampling', 'bundle') and isinstance(value, dict):
            section = data.get(key, {})
            section.update(value)
            data[key] = section
        else:
            data[key] = value
    data['disk'] = {
        'source_store_free_gb': disk_free_gb(utils.store_dir('source-store')),
        'aggregation_store_free_gb': disk_free_gb(utils.store_dir('aggregation-store')) if os.path.isdir(utils.store_dir('aggregation-store')) else disk_free_gb('.'),
    }
    save_status(data)


def print_status(data=None):
    if data is None:
        data = refresh()
    print('Mapterhorn run status')
    print('  updated_at: {}'.format(data.get('updated_at')))
    print('  stage: {}'.format(data.get('stage')))
    print('  run_id: {}'.format(data.get('run_id')))
    print('  shoreline_mask: {}'.format(data.get('shoreline_mask')))
    print('  aggregation_id: {}'.format(data.get('aggregation_id')))
    for kind in ('aggregation', 'downsampling'):
        section = data.get(kind) or {}
        if not section:
            continue
        print('  {}: {}/{} done ({:.1f}%), todo={}, failed={}, eta={}'.format(
            kind,
            section.get('items_done', 0),
            section.get('items_total', 0),
            section.get('percent', 0.0),
            section.get('items_todo', 0),
            section.get('items_failed', 0),
            section.get('eta'),
        ))
    disk = data.get('disk') or {}
    print('  disk free GB: source={}, aggregation={}'.format(
        disk.get('source_store_free_gb'), disk.get('aggregation_store_free_gb')))
    failures = data.get('failures') or []
    if failures:
        print('  failures ({}):'.format(len(failures)))
        for item in failures[:20]:
            print('    - {}: {}'.format(item.get('item'), item.get('error', '')[:120]))
    if data.get('last_error'):
        print('  last_error: {}'.format(data.get('last_error')))


def main():
    data = refresh()
    print_status(data)
    print('\nWrote {}'.format(STATUS_PATH))


if __name__ == '__main__':
    main()
