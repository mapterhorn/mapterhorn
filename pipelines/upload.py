import json

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

if __name__ == '__main__':

    bucket = 'mapterhorn'
    region = 'auto'
    endpoint = 'https://5521f1c60beed398e82b05eabc341142.r2.cloudflarestorage.com/'
    
    # PMTiles

    download_urls = None
    with open('bundle-store/download_urls.json') as f:
        download_urls = json.load(f)

    for item in download_urls['items']:
        print(item['name'])
        filename = item['name']
        directory = f'bundle-store/{filename.replace(".pmtiles", "")}/'
        key = filename
        upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)
    
    # Special files

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

    # Source tarballs

    attribution = None
    with open('bundle-store/attribution.json') as f:
        attribution = json.load(f)
    
    for item in attribution:
        print(item['source'])
        filename = f'{item["source"]}.tar'
        directory = 'tar-store/'
        key = f'sources/{filename}'
        upload_local_resource_to_s3(directory, filename, bucket, key, region, endpoint)
