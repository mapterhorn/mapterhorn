import requests
import json
from multiprocessing import Pool
import os

import utils

SILENT = False
CHUNKSIZE = 1_000_000_000
TMPDIR = '/tmp/'
PROCESSES = 32

def get_file_size(url):
    r = requests.head(url)
    return int(r.headers.get('Content-Length', 0))

def download_range(url, start, end, filepath):
    command = f'curl -r {start}-{end} {url} -o {filepath}'
    out, err = utils.run_command(command, silent=SILENT)
    if not SILENT:
        print('out:', out)
        print('err:', err)

def create_multipart_upload(bucket, key, region):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''
    
    command = f'aws s3api create-multipart-upload --bucket {bucket} --key {key} --region {region}'
    out, err = utils.run_command(command, silent=SILENT)
    if err != '':
        print('err:', err)
        raise Exception(err)
    data = json.loads(out)
    return data.get('UploadId', None)

def upload_part(bucket, key, part_number, filepath, upload_id, region):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''

    command = f'aws s3api upload-part --bucket {bucket} --key {key} --part-number {part_number} --body {filepath} --upload-id "{upload_id}" --region {region}'
    out, err = utils.run_command(command, silent=SILENT)
    if err != '':
        print('err:', err)
        raise Exception(err)
    data = json.loads(out)
    return data.get('ETag', None)

def complete_multipart_upload(bucket, key, upload_id, parts, region):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''
        
    parts = {'Parts': parts}
    command = f'aws s3api complete-multipart-upload --bucket {bucket} --key {key} --upload-id "{upload_id}" --multipart-upload \'{json.dumps(parts)}\' --region {region}'
    out, err = utils.run_command(command, silent=SILENT)
    if err != '':
        print('err:', err)
        raise Exception(err)

def process_range(url, start, end, bucket, key, part_number, part_filepath, upload_id, region):
    download_range(url, start, end, part_filepath)
    etag = upload_part(bucket, key, part_number, part_filepath, upload_id, region)
    os.remove(part_filepath)
    return {'ETag': etag, 'PartNumber': part_number}

def mirror_http_resource_to_s3(url, bucket, key, region, filename):  
    upload_id = create_multipart_upload(bucket, key, region)
    print('upload_id', upload_id)

    full_size = get_file_size(url)
    print('full_size', full_size)

    part_number = 1
    start = 0
    end = start + CHUNKSIZE - 1

    argument_tuples = []
    while start < full_size:
        part_filepath = f'{TMPDIR}/{filename}.part{part_number}'
        argument_tuples.append((url, start, end, bucket, key, part_number, part_filepath, upload_id, region))        

        part_number += 1
        start += CHUNKSIZE
        end += CHUNKSIZE
    
    parts = None
    with Pool(PROCESSES) as pool:
        parts = pool.starmap(process_range, argument_tuples, chunksize=1)
    
    complete_multipart_upload(bucket, key, upload_id, parts, region)

def get_filenames():
    r = requests.get('https://download.mapterhorn.com/download_urls.json')
    data = json.loads(r.text)
    filenames = [item['name'] for item in data['items']]
    filenames += [
        'attribution.json',
        'download_urls.json'
    ]
    return filenames

if __name__ == '__main__':
    filenames = get_filenames()
    for filename in filenames:
        url = F'https://download.mapterhorn.com/{filename}'
        bucket = 'us-west-2.opendata.source.coop'
        key = f'mapterhorn/{filename}'
        region = 'us-west-2'
        mirror_http_resource_to_s3(url, bucket, key, region, filename)
