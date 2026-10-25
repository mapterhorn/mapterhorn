import utils
import sys
import os

def download_from_internet(source):
    urls = []
    with open(f'../source-catalog/{source}/file_list.txt') as f:
        urls = [l.strip() for l in f.readlines()]
    j = 0
    for url in urls:
        j += 1
        if j % 100 == 0:
            print(f'downloaded {j} / {len(urls)}')

        command = f'cd source-store/{source} && wget --no-verbose --continue "{url}"'
        utils.run_command(command, silent=False)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'downloading {source}...')
    else:
        print('source argument missing...')
        exit()

    os.makedirs( f'source-store/{source}/', exist_ok=True)
    download_from_internet(source)

if __name__ == '__main__':
    main()
