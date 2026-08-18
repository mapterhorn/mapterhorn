import sys
import shutil
import utils
import source_marker
from glob import glob
import os


def main():
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print('extracting tarball of source {}...'.format(source))
    else:
        print('Not enough arguments. Usage: source_extract_tarball.py {{source}}')
        exit()

    if source_marker.is_source_ready(source):
        print('{} already READY, skipping extract'.format(source))
        return

    source_marker.begin_download(source)
    tar_path = utils.store_dir('tar-store') + '/{}.tar'.format(source)
    dest = source_marker.source_folder(source)
    if not os.path.isfile(tar_path):
        raise FileNotFoundError(tar_path)
    command = 'tar xf "{}" -C "{}"'.format(tar_path, dest)
    out, err = utils.run_command(command, silent=False, stream=True)
    if err:
        raise RuntimeError('tar extract failed for {}: {}'.format(source, err))

    tif_filepaths = glob('{}/files/*.tif'.format(dest))
    for tif_filepath in tif_filepaths:
        os.replace(tif_filepath, tif_filepath.replace('{}/files/'.format(dest), '{}/'.format(dest)))

    shutil.rmtree('{}/files'.format(dest))
    os.remove('{}/LICENSE.pdf'.format(dest))
    os.remove('{}/metadata.json'.format(dest))
    os.remove('{}/coverage.gpkg'.format(dest))
    source_marker.mark_download_complete(source)
    source_marker.mark_ready(source)


if __name__ == '__main__':
    main()
