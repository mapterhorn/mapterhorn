from glob import glob
import sys
import zipfile
import shutil
from multiprocessing import Pool

import utils

def unzip(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'unzip -o {filepath} -d source-store/{source}/{filename}-tmp/', silent=False)
    utils.run_command(f'rm {filepath}', silent=False)

def un7z(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'7z x -osource-store/{source}/{filename}-tmp/ "{filepath}"', silent=False)
    filepaths_to_remove = None
    if filepath.endswith('.7z'):
        filepaths_to_remove = [filepath]
    else:
        # filepath ends with '.7z.001'
        filepaths_to_remove = [path for path in glob(filepath.replace('.7z.001', '.7z.*')) if not path.endswith('-tmp')]
    for filepath_to_remove in filepaths_to_remove:
        utils.run_command(f'rm "{filepath_to_remove}"', silent=False)

def move_images(filepath, source, suffix):
    image_filepaths = glob(f'{filepath}-tmp/**/*.{suffix}', recursive=True)
    for image_filepath in image_filepaths:
        image_filename = image_filepath.split('/')[-1]
        utils.run_command(f'mv {image_filepath} source-store/{source}/{image_filename}')

def is_7z_head_file(filepath):
    return filepath.endswith('.7z') or filepath.endswith('.7z.001')

def to_cog(filepath_in, filepath_out):
    utils.run_command(f'gdal_translate -of COG -co COMPRESS=LZW -co BLOCKSIZE=512 {filepath_in} {filepath_out}', silent=True)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'unzipping {source}...')
    else:
        print('source argument missing...')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    for filepath in filepaths:
        if zipfile.is_zipfile(filepath):
            unzip(filepath, source)
        elif is_7z_head_file(filepath):
            un7z(filepath, source)
        move_images(filepath, source, 'tif')
        move_images(filepath, source, 'asc')
        move_images(filepath, source, 'xyz')
        text_filepaths = glob(f'source-store/{source}/*.asc') + glob(f'source-store/{source}/*.xyz')
        argument_tuples = [(text_filepath, f'{text_filepath}.tif') for text_filepath in text_filepaths]
        with Pool() as pool:
            pool.starmap(to_cog, argument_tuples)
        for text_filepath in text_filepaths:
            utils.run_command(f'rm {text_filepath}')
        shutil.rmtree(f'{filepath}-tmp')

if __name__ == '__main__':
    main()
