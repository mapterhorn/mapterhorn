import utils
import sys

def download_from_internet(source):
    urls = []
    with open(f'../source-catalog/{source}/file_list.txt') as f:
        line = f.readline().strip()
        while line != '':
            urls.append(line)
            line = f.readline().strip()
    for url in urls:
        filename = url.split('/')[-1]
        command = f'wget --no-verbose -O source-store/{source}/{filename} -c {url}'
        utils.run_command(command, silent=False)

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'downloading {source}...')
    else:
        print('source argument missing...')
        exit()

    utils.create_folder( f'source-store/{source}/')
    download_from_internet(source)

if __name__ == '__main__':
    main()
