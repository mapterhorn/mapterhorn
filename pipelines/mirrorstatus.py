import requests
import json
import time

import upload
import utils

def get_size_by_filename():
    size_by_filename = {}
    r = requests.get('https://download.mapterhorn.com/download_urls.json')
    data = json.loads(r.text)
    for item in data['items']:
        size_by_filename[item['name']] = item['size']
    return size_by_filename

def get_mirrors():
    r = requests.get('https://raw.githubusercontent.com/mapterhorn/mapterhorn/refs/heads/main/distribution/mirrors.json')
    return json.loads(r.text)

def main():   
    last_update = int(time.time())

    size_by_filename = get_size_by_filename()
    mirrors = get_mirrors()

    items = {}

    for filename in size_by_filename:
        print(filename)
        items[filename] = []
        for mirror_name, mirror in mirrors.items():
            url = f'{mirror["base_url"]}{filename}'
            r = requests.head(url, timeout=5)
            mirror_size = int(r.headers.get('Content-Length', 0))
            if mirror_size == size_by_filename[filename]:
                print(f'  found matching filesize on {mirror_name}')
                items[filename].append(mirror_name)
            else:
                print(f'  did not find a matching filesize on {mirror_name}: primary={size_by_filename[filename]}, mirror={mirror_size}')
    
    mirrorstatus = {
        'last_update': last_update,
        'mirrors': mirrors,
        'items': items,
    }

    print(json.dumps(mirrorstatus, indent=2))

    utils.create_folder('bundle-store')

    with open('bundle-store/mirrorstatus.json', 'w') as f:
        json.dump(mirrorstatus, f, indent=2)

    directory = 'bundle-store'
    filename = 'mirrorstatus.json'
    key = filename
    bucket = 'mapterhorn'
    region = 'auto'
    endpoint = 'https://5521f1c60beed398e82b05eabc341142.r2.cloudflarestorage.com/'
    upload.upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)


if __name__ == '__main__':
    while True:
        try:
            main()
        except requests.exceptions.ReadTimeout as e:
            print(e)
            print('Sleeping for 1 minute...')
            time.sleep(60)
            continue
        sleep_minutes = 15
        for i in range(2 * sleep_minutes):
            print(f'Sleeping for {(2 * sleep_minutes - i) / 2} more minutes...')
            time.sleep(30)
