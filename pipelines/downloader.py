from glob import glob
import shutil
import os
import time
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor

def get_folder_size(path):
    return sum(f.stat().st_size for f in Path(path).rglob('*') if f.is_file())

MAX_TMP_SOURCE_SIZE = int(os.environ.get('MAPTERHORN_MAX_TMP_SOURCE_SIZE', 100)) * 1024 ** 3
SOFTLINK_SOURCE = bool(int(os.environ.get('MAPTERHORN_SOFTLINK_SOURCE', 0)))

def prune_cache(last_access):
    tmp_source_size = get_folder_size('tmp-store/source')

    if tmp_source_size < MAX_TMP_SOURCE_SIZE:
        return

    while True:
        ready_target_filepaths = set({})
        for item in glob('tmp-store/ready/*'):
            lines = []
            with open(item) as f:
                lines = f.readlines()
                lines = lines[1:]
            for line in lines:
                source, filename, _ = line.split(',')
                target_folder = f'tmp-store/source/{source}'
                target_filepath = f'{target_folder}/{filename}'
                ready_target_filepaths.add(target_filepath)
        filepaths = glob('tmp-store/source/*/*')
        filepaths = sorted(filepaths, key=lambda f: last_access.get(f, 0))

        print('Start removing unused files...')
        for filepath in filepaths:
            if filepath in ready_target_filepaths:
                continue
            tmp_source_size -= os.path.getsize(filepath)
            os.remove(filepath)
            print(f'Removed {filepath}.')
        if tmp_source_size < MAX_TMP_SOURCE_SIZE:
            print('Freed enough space.')
            return
        print('Did not free enough space. Sleeping...')
        time.sleep(1)

def local_copy(source, filename, target_filepath):
    if SOFTLINK_SOURCE:
        os.symlink(os.path.realpath(f'source-store/{source}/{filename}'), target_filepath)
    else:
        shutil.copy(f'source-store/{source}/{filename}', target_filepath)

def main():
    os.makedirs('tmp-store/source', exist_ok=True)
    os.makedirs('tmp-store/ready', exist_ok=True)
    last_access = {}
    iteration = 0

    while True:
        prune_cache(last_access)
        items = glob('tmp-store/queue/*.csv')
        if len(items) == 0:
            print('empty queue...')
            time.sleep(1.0)
            continue
        items = sorted(items, key=lambda f: os.path.getmtime(f))
        for item in items:
            iteration += 1
            lines = []
            with open(item) as f:
                lines = f.readlines()
            lines = lines[1:] # skip header

            sources = []
            filenames = []
            target_filepaths = []
            for line in lines:
                source, filename, _ = line.split(',')
                target_folder = f'tmp-store/source/{source}'
                target_filepath = f'{target_folder}/{filename}'
                last_access[target_filepath] = iteration
                if os.path.isfile(target_filepath):
                    continue
                os.makedirs(target_folder, exist_ok=True)
                sources.append(source)
                filenames.append(filename)
                target_filepaths.append(target_filepath)

            print(f'Start copying {len(filenames)} files...')
            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(local_copy, sources, filenames, target_filepaths))
            print(f'Done.')

            os.rename(item, item.replace('tmp-store/queue', 'tmp-store/ready'))
            

if __name__ == '__main__':
    main()