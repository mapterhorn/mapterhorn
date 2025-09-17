from glob import glob
import zipfile
import shutil
import os

import utils

SILENT = False

def main():
    source = 'ro'
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    for filepath in filepaths:
        if not zipfile.is_zipfile(filepath):
            continue

        filename = filepath.split('/')[-1]
        utils.run_command(f'unzip -o "{filepath}" -d "source-store/{source}/{filename}-tmp/"', silent=SILENT)
        utils.run_command(f'rm "{filepath}"', silent=False)

        image_filepaths = glob(f'{filepath}-tmp/**/w001001.adf', recursive=True)
        assert len(image_filepaths) == 1
        filepath_in = image_filepaths[0]
        filename_out = filename.replace('.zip', '.tif')
        filepath_out = f'source-store/{source}/{filename_out}'
        
        utils.run_command(f'gdal_translate -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath_in}" "{filepath_out}"', silent=SILENT)

        
        tmpdir = f'{filepath}-tmp'
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)

if __name__ == '__main__':
    main()
