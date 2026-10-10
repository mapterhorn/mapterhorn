#!/usr/bin/env python3

import requests
import xml.etree.ElementTree as ET
from pathlib import Path

S3_INDEX_URL = 'https://prd-tnm.s3.amazonaws.com/?prefix=StagedProducts/Elevation/1m/Projects/&delimiter=/'
OUTPUT_FILE = Path(__file__).parent / 'full_file_list.txt'


def fetch_project_directories():
    print('Fetching project directories from S3...')
    response = requests.get(S3_INDEX_URL)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

    directories = []
    for prefix in root.findall('.//s3:CommonPrefixes/s3:Prefix', namespace):
        dir_path = prefix.text
        if dir_path and dir_path != 'StagedProducts/Elevation/1m/Projects/':
            directories.append(dir_path)

    print(f'Found {len(directories)} project directories')
    return directories


def fetch_download_links(project_dir):
    download_links_url = (
        f'https://prd-tnm.s3.amazonaws.com/{project_dir}0_file_download_links.txt'
    )

    try:
        response = requests.get(download_links_url, timeout=30)
        response.raise_for_status()

        urls = []
        for line in response.text.strip().split('\n'):
            url = line.strip()
            if url and url.startswith('http'):
                urls.append(url)

        return urls
    except requests.exceptions.RequestException as e:
        print(f'Warning: Could not fetch {download_links_url}: {e}')
        return []


def main():
    directories = fetch_project_directories()
    print(f'Found {len(directories)} project directories\n')

    all_urls = []

    for i, directory in enumerate(directories, 1):
        project_name = directory.rstrip('/').split('/')[-1]
        print(f'[{i}/{len(directories)}] Processing {project_name}...')

        urls = fetch_download_links(directory)
        all_urls.extend(urls)
        print(f'  Found {len(urls)} URLs (total: {len(all_urls)})')

    print(f'\nWriting {len(all_urls)} URLs to {OUTPUT_FILE}...')
    with open(OUTPUT_FILE, 'w') as f:
        for url in all_urls:
            f.write(f'{url}\n')

    print('Done!')


if __name__ == '__main__':
    main()
