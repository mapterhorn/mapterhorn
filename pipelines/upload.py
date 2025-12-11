import json

import requests

import utils

SILENT = False

def upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''
    command = f'aws s3 cp {directory}/{filename} s3://{bucket}/{key} --region {region} --endpoint "{endpoint}"'
    _, err = utils.run_command(command, silent=SILENT)
    if err != '':
        print('err:', err)
        raise Exception(err)

def handle_pmtiles(bucket, region, endpoint):
    r = requests.get('https://download.mapterhorn.com/download_urls.json')
    if r.status_code != 200:
        raise Exception('Error: could not get online download_urls.json')
    
    online_download_urls = json.loads(r.text)
    online_items = {}
    for item in online_download_urls['items']:
        online_items[item['name']] = item
    
    download_urls = None
    with open('bundle-store/download_urls.json') as f:
        download_urls = json.load(f)

    for item in download_urls['items']:
        print(item['name'])
        if item['name'] in online_items and online_items[item['name']]['md5sum'] == item['md5sum']:
            print('Nothing changed. Skipping...')
            continue

        filename = item['name']
        directory = f'bundle-store/{filename.replace(".pmtiles", "")}/'
        key = filename
        upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)

def handle_tarballs(bucket, region, endpoint):

    r = requests.get('https://download.mapterhorn.com/attribution.json')
    if r.status_code != 200:
        raise Exception('Error: could not get online attribution.json')

    online_attribution = json.loads(r.text)
    online_source_md5sums = {}
    for item in online_attribution:
        online_source_md5sums[item['source']] = item['tarball_md5sum']

    attribution = None
    with open('bundle-store/attribution.json') as f:
        attribution = json.load(f)
    
    for item in attribution:
        print(item['source'])
        if item['source'] != 'at1' and item['source'] in online_source_md5sums and online_source_md5sums[item['source']] == item['tarball_md5sum']:
            print('Nothing changed. Skipping...')
            continue

        filename = f'{item["source"]}.tar'
        directory = 'tar-store/'
        key = f'sources/{filename}'
        upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)
    
if __name__ == '__main__':

    bucket = 'mapterhorn'
    region = 'auto'
    endpoint = 'https://5521f1c60beed398e82b05eabc341142.r2.cloudflarestorage.com/'
    

    handle_pmtiles(bucket, region, endpoint)

    handle_tarballs(bucket, region, endpoint)
    exit()
    directory = 'bundle-store/'

    filename = 'attribution.json'
    key = filename
    upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)

    filename = 'download_urls.json'
    key = filename
    upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)

    filename = 'coverage.pmtiles'
    key = filename
    upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)


