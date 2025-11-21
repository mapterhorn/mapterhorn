import json
import requests
from multiprocessing import Pool

import utils

SILENT=True
PROCESSES = 32

def has_expected_size(url, expected_size):
    r = requests.head(url)
    actual_size = int(r.headers.get('Content-Length', -1))
    return actual_size == expected_size

def has_expected_md5sum(url, expected_md5sum):
    command = f'wget {url} -O - | md5sum'
    out, _ = utils.run_command(command, silent=SILENT)
    parts = out.split(' ')
    assert len(parts) > 0
    return parts[0] == expected_md5sum

def has_expected_size_and_md5sum(url, expected_size, expected_md5sum):
    if not has_expected_size(url, expected_size):
        return False
    if not has_expected_md5sum(url, expected_md5sum):
        return False
    return True

def print_check(url, expected_size, expected_md5sum):
    if has_expected_size_and_md5sum(url, expected_size, expected_md5sum):
        print(url, 'good')
    else:
        print(url, 'bad')

def main():
    r = requests.get('https://download.mapterhorn.com/download_urls.json')
    data = json.loads(r.text)
    
    base_url = 'https://download.mapterhorn.com/' # Cloudflare
    # base_url = 'https://nbg1.your-objectstorage.com/mapterhorn/' # Hetzner
    # base_url = 'https://data.source.coop/mapterhorn/mapterhorn/' # Source Coop

    argument_tuples = []
    for item in data['items']:
        url = f'{base_url}{item['name']}'
        argument_tuples.append((url, item['size'], item['md5sum']))

    with Pool(PROCESSES) as pool:
        pool.starmap(print_check, argument_tuples, chunksize=1)

if __name__ == '__main__':
    main()