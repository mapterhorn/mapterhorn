import requests
import json
from multiprocessing import Pool
import os
import subprocess

SILENT = False
CHUNKSIZE = 1_000_000_000
TMPDIR = '/tmp/'
PROCESSES = 16

def run_command(command, silent=True):
    if not silent:
        print(command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    err = stderr.decode()
    if err != '' and not silent:
        print(err)
    out = stdout.decode()
    if out != '' and not silent:
        print(out)
    return out, err

def get_file_size(url):
    r = requests.head(url)
    if r.headers.get('Content-Encoding', None) == 'gzip':
        return CHUNKSIZE
    return int(r.headers.get('Content-Length', 0))

def download_range(url, start, end, filepath):
    command = f'curl -r {start}-{end} {url} -o {filepath}'
    out, err = run_command(command, silent=SILENT)
    if not SILENT:
        print('out:', out)
        print('err:', err)

def create_multipart_upload(bucket, key, region, endpoint):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''
    
    command = f'aws s3api create-multipart-upload --bucket {bucket} --key {key} --region {region} --endpoint "{endpoint}"'
    out, err = run_command(command, silent=SILENT)
    if err != '':
        print('err:', err)
        raise Exception(err)
    data = json.loads(out)
    return data.get('UploadId', None)

def upload_part(bucket, key, part_number, filepath, upload_id, region, endpoint):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''

    command = f'aws s3api upload-part --bucket {bucket} --key {key} --part-number {part_number} --body {filepath} --upload-id "{upload_id}" --region {region} --endpoint "{endpoint}"'
    out, err = run_command(command, silent=SILENT)
    if err != '':
        print('err:', err)
        raise Exception(err)
    data = json.loads(out)
    return data.get('ETag', None)

def complete_multipart_upload(bucket, key, upload_id, parts, region, endpoint):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''
        
    parts = {'Parts': parts}
    command = f'aws s3api complete-multipart-upload --bucket {bucket} --key {key} --upload-id "{upload_id}" --multipart-upload \'{json.dumps(parts)}\' --region {region} --endpoint "{endpoint}"'
    _, err = run_command(command, silent=SILENT)
    if err != '':
        print('err:', err)
        raise Exception(err)

def process_range(url, start, end, bucket, key, part_number, part_filepath, upload_id, region, endpoint):
    retries = 0
    max_retries = 3
    while True:
        try:
            download_range(url, start, end, part_filepath)
            etag = upload_part(bucket, key, part_number, part_filepath, upload_id, region, endpoint)
            os.remove(part_filepath)
            return {'ETag': etag, 'PartNumber': part_number}
        except Exception as e:
            if retries < max_retries:
                print(f'retries={retries}, max_retries={max_retries}, err={e}')
                retries += 1
            else:
                raise Exception(f'max retries reached, err={e}')

def mirror_http_resource_to_s3(url, bucket, key, region, endpoint, filename):  
    upload_id = create_multipart_upload(bucket, key, region, endpoint)
    print('upload_id', upload_id)

    full_size = get_file_size(url)
    print('full_size', full_size)

    part_number = 1
    start = 0
    end = start + CHUNKSIZE - 1

    argument_tuples = []
    while start < full_size:
        part_filepath = f'{TMPDIR}/{filename}.part{part_number}'
        argument_tuples.append((url, start, end, bucket, key, part_number, part_filepath, upload_id, region, endpoint))        

        part_number += 1
        start += CHUNKSIZE
        end += CHUNKSIZE
    
    parts = None
    with Pool(PROCESSES) as pool:
        parts = pool.starmap(process_range, argument_tuples, chunksize=1)
    
    complete_multipart_upload(bucket, key, upload_id, parts, region, endpoint)

def get_filenames(mirror_base_url):
    filenames = []

    mapterhorn_r = requests.get('https://download.mapterhorn.com/download_urls.json')
    if mapterhorn_r.status_code != 200:
        raise Exception('Failed to load download_urls.json from mapterhorn.com')
    mapterhorn_data = json.loads(mapterhorn_r.text)
    mapterhorn_name_to_md5sum = {item['name']: item['md5sum'] for item in mapterhorn_data['items']}

    mirror_r = requests.get(f'{mirror_base_url}download_urls.json')
    mirror_name_to_md5sum = {}
    if mirror_r.status_code == 200:
        mirror_data = json.loads(mirror_r.text)
        mirror_name_to_md5sum = {item['name']: item['md5sum'] for item in mirror_data['items']}
    
    for name in mapterhorn_name_to_md5sum:
        if name not in mirror_name_to_md5sum:
            filenames.append(name)
        else:
            if mapterhorn_name_to_md5sum[name] != mirror_name_to_md5sum[name]:
                filenames.append(name)

    return filenames

def main():

    # Hetzner
    mirror_base_url = 'https://nbg1.your-objectstorage.com/mapterhorn/'
    region = 'nbg1'
    bucket = 'mapterhorn'
    endpoint = 'https://nbg1.your-objectstorage.com/'
    prefix = '' 

    # # Source Coop
    # mirror_base_url = 'https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/mapterhorn/mapterhorn/'
    # region = 'us-west-2'
    # bucket = 'us-west-2.opendata.source.coop'
    # endpoint = f'https://s3.{region}.amazonaws.com'
    # prefix = 'mapterhorn/mapterhorn/'

    filenames = get_filenames(mirror_base_url)

    if len(filenames) == 0:
        print('nothing to do...')
        return
    
    filenames += ['attribution.json', 'download_urls.json']

    for filename in filenames:
        url = f'https://download.mapterhorn.com/{filename}'
        key = f'{prefix}{filename}'
        mirror_http_resource_to_s3(url, bucket, key, region, endpoint, filename)

if __name__ == '__main__':
    main()
