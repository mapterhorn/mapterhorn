from glob import glob
import sys
import zipfile
import shutil
# from multiprocessing import Pool
import os

import utils

SILENT = False

def unzip(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'unzip -o "{filepath}" -d "source-store/{source}/{filename}-tmp/"', silent=SILENT)
    utils.run_command(f'rm "{filepath}"', silent=False)

def un7z(filepath, source):
    filename = filepath.split('/')[-1]
    utils.run_command(f'7z x -osource-store/{source}/{filename}-tmp/ "{filepath}"', silent=SILENT)
    filepaths_to_remove = None
    if filepath.endswith('.7z'):
        filepaths_to_remove = [filepath]
    else:
        # filepath ends with '.7z.001'
        filepaths_to_remove = [path for path in glob(filepath.replace('.7z.001', '.7z.*')) if not path.endswith('-tmp')]
    for filepath_to_remove in filepaths_to_remove:
        utils.run_command(f'rm "{filepath_to_remove}"', silent=SILENT)

def translate_images(filepath, source, suffix):
    image_filepaths = glob(f'{filepath}-tmp/**/*.{suffix}', recursive=True)
    for image_filepath in image_filepaths:
        image_filename = image_filepath.split('/')[-1]

        filepath_in = image_filepath
        filepath_out = f'source-store/{source}/{image_filename}'
        if filepath_in.endswith('.tif'):
            pass
        elif filepath_in.endswith('.TIF'):
            filepath_out = filepath_out[:-3] + 'tif'
        elif filepath_in.endswith('.xyz'):
            filepath_out = filepath_out[:-3] + 'tif'
        elif filepath_in.endswith('.asc'):
            filepath_out = filepath_out[:-3] + 'tif'

        # utils.run_command(f'mv "{image_filepath}" "source-store/{source}/{image_filename}"', silent=SILENT)
        utils.run_command(f'gdal_translate -of COG -co COMPRESS=LZW -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BLOCKSIZE=512 -co BIGTIFF=YES "{filepath_in}" "{filepath_out}"', silent=False)

def is_7z_head_file(filepath):
    return filepath.endswith('.7z') or filepath.endswith('.7z.001')

# def to_cog(filepath):
#     filepath_in = None
#     filepath_out = None
#     if filepath.endswith('.tif') or filepath.endswith('.TIF'):
#         utils.run_command(f'mv {filepath} {filepath}.bak', silent=False)
#         filepath_in = f'{filepath}.bak'
#         filepath_out = filepath
#     elif filepath.endswith('.xyz'):
#         filepath_in = filepath
#         filepath_out = filepath.replace('.xyz', '.tif')
#     elif filepath.endswith('.asc'):
#         filepath_in = filepath
#         filepath_out = filepath.replace('.asc', '.tif')
    
#     utils.run_command(f'gdal_translate -of COG -co COMPRESS=LZW -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BLOCKSIZE=512 -co BIGTIFF=YES "{filepath_in}" "{filepath_out}"', silent=False)
#     utils.run_command(f'rm "{filepath_in}"', silent=False)

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

        translate_images(filepath, source, 'tif')
        translate_images(filepath, source, 'TIF')
        translate_images(filepath, source, 'asc')
        translate_images(filepath, source, 'xyz')

        # input_filepaths = []
        # input_filepaths += glob(f'source-store/{source}/*.tif')
        # input_filepaths += glob(f'source-store/{source}/*.TIF')
        # input_filepaths += glob(f'source-store/{source}/*.xyz')
        # input_filepaths += glob(f'source-store/{source}/*.asc')

        # argument_tuples = [(path,) for path in input_filepaths]
        # with Pool() as pool:
        #     pool.starmap(to_cog, argument_tuples)
        
        tmpdir = f'{filepath}-tmp'
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)

if __name__ == '__main__':
    main()
