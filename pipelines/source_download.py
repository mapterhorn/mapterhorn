import utils
import source_marker
import sys
import os


def catalog_urls(source):
    urls = []
    list_path = utils.catalog_path(source, 'file_list.txt')
    if not os.path.isfile(list_path):
        return urls
    with open(list_path) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            urls.append(line)
    return urls


def download_from_internet(source):
    urls = catalog_urls(source)
    if len(urls) == 0:
        print('no download URLs in file_list.txt for {}'.format(source))
        return False
    total = len(urls)
    folder = utils.store_dir('source-store') + '/{}'.format(source)
    for j, url in enumerate(urls, start=1):
        print('[{}/{}] {}'.format(j, total, url))
        utils.wget_download(url, cwd=folder)
    return True


def main():
    source = None
    if len(sys.argv) > 1:
        source = sys.argv[1]
        print('downloading {}...'.format(source))
    else:
        print('source argument missing...')
        exit()

    if source_marker.is_download_complete(source):
        print('{} already downloaded ({}), skipping'.format(source, source_marker.DOWNLOAD_MARKER))
        return

    source_marker.begin_download(source)
    if download_from_internet(source):
        source_marker.mark_download_complete(source)
        print('{} download complete'.format(source))


if __name__ == '__main__':
    main()
