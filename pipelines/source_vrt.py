import sys

import utils

def create_vrt(source):
    lines = None
    with open(f'source-store/{source}/bounds.csv') as f:
        lines = f.readlines()

    lines = [l.strip() for l in lines[1:]]

    with open(f'source-store/{source}/file_list.txt', 'w') as f:
        for line in lines:
            filename = line.split(',')[0]
            f.write(f'{filename}\n')

    utils.run_command(f'cd source-store/{source}/ && gdalbuildvrt -o mosaic.vrt -input_file_list file_list.txt -overwrite', silent=False)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'creating vrt for {source}...')
    else:
        print('source argument missing...')
        exit()
    create_vrt(source)

if __name__ == '__main__':
    main()

