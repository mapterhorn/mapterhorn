import utils
import sys
from glob import glob
import zipfile
import os
import shutil

def download_from_internet(source):
    urls = []
    with open(f'../source-catalog/{source}/file_list.txt') as f:
        urls = [l.strip() for l in f.readlines()]
    j = 0
    for url in urls:
        j += 1
        if j % 100 == 0:
            print(f'downloaded {j} / {len(urls)}')

        # filepaths_before = glob(f'source-store/{source}/*')
        command = f'cd source-store/{source} && wget --no-verbose --continue "{url}"'
        utils.run_command(command, silent=False)
        # filepaths_after = glob(f'source-store/{source}/*')
        # new_filepaths = list(set(filepaths_after) - set(filepaths_before))
        # if len(new_filepaths) == 1:
        #     filepath = new_filepaths[0]
        #     if zipfile.is_zipfile(filepath):
        #         utils.run_command(f'unzip -o {filepath} -d source-store/{source}/tmp/', silent=False)
        #         utils.run_command(f'rm {filepath}', silent=False)
        #         tif_filepaths = glob(f'source-store/{source}/tmp/**/*.tif', recursive=True)
        #         for tif_filepath in tif_filepaths:
        #             tif_filename = tif_filepath.split('/')[-1]
        #             utils.run_command(f'mv {tif_filepath} source-store/{source}/{tif_filename}')
        #         for entry in os.scandir(f'source-store/{source}/'):
        #             if entry.is_dir():
        #                 shutil.rmtree(entry.path)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'downloading {source}...')
    else:
        print('source argument missing...')
        exit()

    utils.create_folder( f'source-store/{source}/')
    download_from_internet(source)

if __name__ == '__main__':
    main()
