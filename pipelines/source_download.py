import utils
import sys

def download_from_internet(source):
    urls = []
    with open(utils.catalog_path(source, 'file_list.txt')) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            urls.append(line)
    if len(urls) == 0:
        print('no download URLs in file_list.txt for {}'.format(source))
        return
    total = len(urls)
    for j, url in enumerate(urls, start=1):
        print('[{}/{}] {}'.format(j, total, url))
        utils.wget_download(url, cwd=utils.store_dir('source-store') + '/{}'.format(source))

def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print('downloading {}...'.format(source))
    else:
        print('source argument missing...')
        exit()

    utils.create_folder(utils.store_dir('source-store') + '/{}/'.format(source))
    download_from_internet(source)

if __name__ == '__main__':
    main()
