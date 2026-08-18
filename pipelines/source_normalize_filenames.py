from glob import glob
import sys
import shutil
import os
import source_marker
import utils

def normalize_filename(filename):
    characters_to_remove = [
        '(',
        ')',
        ',',
        '[',
        ']',
        '{',
        '}',
        '&',
        '#',
        '%',
        '$',
        '@',
    ]
    for character in characters_to_remove:
        filename = filename.replace(character, '_')

    # GEBCO and similar products embed dots in the basename (e.g. n0.0_s-90.0)
    if '.' in filename:
        name, ext = os.path.splitext(filename)
        # Keep only the final extension; replace other dots
        name = name.replace('.', '_')
        # Collapse query-string leftovers if wget kept ?download=1 in the name
        if '?' in name:
            name = name.split('?', 1)[0]
        if '?' in ext:
            ext = ext.split('?', 1)[0]
        filename = name + ext
    return filename

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print('normalizing filenames of {}...'.format(source))
    else:
        print('source argument missing...')
        exit()

    source_marker.require_download_complete(source)
    
    filepaths = sorted(glob(utils.store_dir('source-store') + '/{}/*'.format(source)))

    for filepath in filepaths:
        if os.path.isdir(filepath):
            continue
        filename = filepath.split('/')[-1]
        if filename == 'bounds.csv' or source_marker.is_marker_filename(filename):
            continue
        normalized_filename = normalize_filename(filename)
        normalized_filepath = utils.store_dir('source-store') + '/{}/{}'.format(source, normalized_filename)
        if filepath != normalized_filepath:
            shutil.move(filepath, normalized_filepath)

if __name__ == '__main__':
    main()
