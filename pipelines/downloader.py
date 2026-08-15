from glob import glob
import shutil
import os
import time
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor

import utils

def get_folder_size(path):
    return sum(f.stat().st_size for f in Path(path).rglob('*') if f.is_file())

MAX_TMP_CACHE_SIZE = int(os.environ.get('MAPTERHORN_MAX_TMP_CACHE_SIZE', 100)) * 1024 ** 3
SOFTLINK_DOWNLOADS = bool(int(os.environ.get('MAPTERHORN_SOFTLINK_DOWNLOADS', 0)))

def parse_line_downsampling(line):
    filename = line.strip()
    file_z, file_x, file_y, _ = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
    pmtiles_folder = utils.get_pmtiles_folder(file_x, file_y, file_z)
    source_filepath = f'{pmtiles_folder}/{filename}'
    target_folder = f'tmp-store/pmtiles/{pmtiles_folder.replace("pmtiles-store", "")}'
    target_filepath = f'{target_folder}/{filename}'
    return source_filepath, target_folder, target_filepath

def parse_line_aggregation(line):
    source, filename, _ = line.strip().split(',')
    source_filepath = f'source-store/{source}/{filename}'
    target_folder = f'tmp-store/source/{source}'
    target_filepath = f'{target_folder}/{filename}'
    return source_filepath, target_folder, target_filepath

def prune_cache(last_access):
    tmp_cache_size = get_folder_size('tmp-store/source') + get_folder_size('tmp-store/pmtiles')

    if tmp_cache_size < MAX_TMP_CACHE_SIZE:
        return

    while True:
        ready_target_filepaths = set({})
        for item in glob('tmp-store/ready/*'):
            lines = []
            with open(item) as f:
                lines = f.readlines()
                lines = lines[1:]
            for line in lines:
                target_filepath = ''

                if item.endswith('-aggregation.csv'):
                    _, target_folder, target_filepath = parse_line_aggregation(line)
                else:
                    _, target_folder, target_filepath = parse_line_downsampling(line)

                ready_target_filepaths.add(target_filepath)
        filepaths = glob('tmp-store/source/*/*') + glob('tmp-store/pmtiles/*.pmtiles') + glob('tmp-store/pmtiles/*/*.pmtiles')
        filepaths = sorted(filepaths, key=lambda f: last_access.get(f, 0))

        print('Start removing unused files...')
        for filepath in filepaths:
            if filepath in ready_target_filepaths:
                continue
            tmp_cache_size -= os.path.getsize(filepath)
            os.remove(filepath)
            print(f'Removed {filepath}.')
        if tmp_cache_size < MAX_TMP_CACHE_SIZE:
            print('Freed enough space.')
            return
        print('Did not free enough space. Sleeping...')
        time.sleep(1)

def local_copy(source_filepath, target_filepath):
    if SOFTLINK_DOWNLOADS:
        os.symlink(os.path.realpath(source_filepath), target_filepath)
    else:
        shutil.copy(source_filepath, target_filepath)

def process_item(item, last_access, iteration):
    lines = []
    with open(item) as f:
        lines = f.readlines()
    lines = lines[1:] # skip header

    total_size = 0

    source_filepaths = []
    target_filepaths = []
    for line in lines:
        source_filepath = ''
        target_folder = ''
        target_filepath = ''
        
        if item.endswith('-aggregation.csv'):
            source_filepath, target_folder, target_filepath = parse_line_aggregation(line)
        else:
            source_filepath, target_folder, target_filepath = parse_line_downsampling(line)

        last_access[target_filepath] = iteration
        if os.path.isfile(target_filepath):
            continue
        
        os.makedirs(target_folder, exist_ok=True)
        source_filepaths.append(source_filepath)
        target_filepaths.append(target_filepath)
        total_size += os.path.getsize(source_filepath)

    if len(source_filepaths) == 0:
        return
    print(f'Start copying {len(lines)} files ({(total_size / 1024 ** 2):.0f} MiB)...')
    tic = time.time()
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(local_copy, source_filepaths, target_filepaths))
    duration = time.time() - tic
    print(f'Done in {duration:.1f} s ({(total_size / duration / 1024 ** 2):.1f} MiB/s).')

def main():
    os.makedirs('tmp-store/source', exist_ok=True)
    os.makedirs('tmp-store/pmtiles', exist_ok=True)
    os.makedirs('tmp-store/ready', exist_ok=True)
    last_access = {}
    iteration = 0

    while True:
        items = glob('tmp-store/queue/*.csv')
        if len(items) == 0:
            print('empty queue...')
            time.sleep(1.0)
            continue
        items = sorted(items, key=lambda f: os.path.getmtime(f))
        for item in items:
            prune_cache(last_access)
            iteration += 1
            process_item(item, last_access, iteration)
            os.rename(item, item.replace('tmp-store/queue', 'tmp-store/ready'))
            

if __name__ == '__main__':
    main()