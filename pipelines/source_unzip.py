# /// script
# requires-python = ">=3.13"
# dependencies = [
# ]
#
# [tool.pixi.workspace]
# channels = ["conda-forge"]
#
# [tool.pixi.dependencies]
# gdal = "==3.13.3"
# unzip = "*"
# 7zip = "*"
# ///

from glob import glob
import sys
import zipfile
import shutil
import os
from multiprocessing import Pool
import subprocess

def run_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout.decode(), stderr.decode()

def unzip(filepath, source):
    filename = filepath.split('/')[-1]
    run_command(f'unzip -o "{filepath}" -d "source-store/{source}/{filename}-tmp/"')
    run_command(f'rm "{filepath}"')

def un7z(filepath, source):
    filename = filepath.split('/')[-1]
    run_command(f'7z x -osource-store/{source}/{filename}-tmp/ "{filepath}"')
    filepaths_to_remove = None
    if filepath.endswith('.7z'):
        filepaths_to_remove = [filepath]
    else:
        # filepath ends with '.7z.001'
        filepaths_to_remove = [path for path in glob(filepath.replace('.7z.001', '.7z.*')) if not path.endswith('-tmp')]
    for filepath_to_remove in filepaths_to_remove:
        run_command(f'rm "{filepath_to_remove}"')

def translate_image(filepath_in, filepath_out, j, total):
    if j % 1000 == 0:
        print(f'{j} / {total}')
    run_command(f'gdal_translate -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath_in}" "{filepath_out}"')

def translate_images(filepath, source, suffix):
    print(f'translate .{suffix} images...')
    # suffix = 'asc' or 'tif' u.s.w. (without dot)
    image_filepaths = glob(f'{filepath}-tmp/**/*.{suffix}', recursive=True)
    
    argument_tuples = []
    j = 0
    for image_filepath in image_filepaths:
        image_filename = image_filepath.split('/')[-1]

        filepath_in = image_filepath
        filepath_out = f'source-store/{source}/{image_filename}'
        suffix_length = len(suffix)
        filepath_out = filepath_out[:-suffix_length] + 'tif'
        argument_tuples.append((filepath_in, filepath_out, j, len(image_filepaths)))
        j += 1

    with Pool() as pool:
        pool.starmap(translate_image, argument_tuples, chunksize=1)

def is_7z_head_file(filepath):
    return filepath.endswith('.7z') or filepath.endswith('.7z.001')


def main(source):
    filepaths = sorted(glob(f'source-store/{source}/*'))

    for filepath in filepaths:
        if zipfile.is_zipfile(filepath):
            unzip(filepath, source)
        elif is_7z_head_file(filepath):
            un7z(filepath, source)

        translate_images(filepath, source, 'tif')
        translate_images(filepath, source, 'TIF')
        translate_images(filepath, source, 'asc')
        translate_images(filepath, source, 'ASC')
        translate_images(filepath, source, 'xyz')
        translate_images(filepath, source, 'grd')
        translate_images(filepath, source, 'img')
        
        tmpdir = f'{filepath}-tmp'
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)

if __name__ == '__main__':
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'unzipping {source}...')
    else:
        print('source argument missing...')
        exit()
    main(source)
