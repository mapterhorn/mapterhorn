# Download GMRT GridServer tiles with stable filenames.
from urllib.parse import parse_qs, urlparse
import os
import sys

import source_marker
import utils


def filename_from_url(url):
    qs = parse_qs(urlparse(url).query)
    minlon = qs.get('minlongitude', ['x'])[0]
    maxlon = qs.get('maxlongitude', ['x'])[0]
    minlat = qs.get('minlatitude', ['y'])[0]
    maxlat = qs.get('maxlatitude', ['y'])[0]
    return 'gmrt_{}_{}_{}_{}.tif'.format(minlat, maxlat, minlon, maxlon)


def catalog_urls(source):
    urls = []
    list_path = utils.catalog_path(source, 'file_list.txt')
    with open(list_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            urls.append(line)
    return urls


def download(source):
    if source_marker.is_download_complete(source):
        print('{} already downloaded ({}), skipping'.format(source, source_marker.DOWNLOAD_MARKER))
        return

    urls = catalog_urls(source)
    if len(urls) == 0:
        print('no download URLs in file_list.txt for {}'.format(source))
        return

    source_marker.begin_download(source)
    out_dir = source_marker.source_folder(source)
    print('downloading {} GMRT tiles...'.format(len(urls)))
    for i, url in enumerate(urls):
        dest = '{}/{}'.format(out_dir, filename_from_url(url))
        print('[{}/{}] {}'.format(i + 1, len(urls), dest))
        utils.wget_download(url, dest=dest)
    source_marker.mark_download_complete(source)
    print('done')


def main():
    if len(sys.argv) < 2:
        print('usage: source_gmrt_download.py <source>')
        exit(1)
    download(sys.argv[1])


if __name__ == '__main__':
    main()
