from glob import glob
import zipfile
import shutil
import os

import source_marker
import utils

SILENT = False


def main():
    source = 'ro'
    source_marker.require_download_complete(source)
    source_marker.begin_extract(source)

    folder = utils.store_dir('source-store') + '/{}'.format(source)
    filepaths = sorted(glob('{}/*'.format(folder)))

    for filepath in filepaths:
        if not zipfile.is_zipfile(filepath):
            continue

        filename = filepath.split('/')[-1]
        tmpdir = filepath + '-tmp'
        utils.create_folder(tmpdir)
        utils.run_command(
            'unzip -o "{}" -d "{}"'.format(filepath, tmpdir), silent=SILENT)
        utils.run_command('rm "{}"'.format(filepath), silent=False)

        image_filepaths = glob('{}/**/w001001.adf'.format(tmpdir), recursive=True)
        assert len(image_filepaths) == 1
        filepath_in = image_filepaths[0]
        filename_out = filename.replace('.zip', '.tif')
        filepath_out = '{}/{}'.format(folder, filename_out)

        utils.run_command(
            'gdal_translate -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES '
            '-co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{}" "{}"'.format(
                filepath_in, filepath_out),
            silent=SILENT,
        )

        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)


if __name__ == '__main__':
    main()
