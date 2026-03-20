from glob import glob
import sys
import shutil

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
    return filename

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'normalizing filenames of {source}...')
    else:
        print('source argument missing...')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*'))

    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        normalized_filename = normalize_filename(filename)
        normalized_filepath = f'source-store/{source}/{normalized_filename}'
        shutil.move(filepath, normalized_filepath)

if __name__ == '__main__':
    main()
