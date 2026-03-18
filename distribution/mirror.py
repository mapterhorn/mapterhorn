import subprocess
import os
import requests
import json

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

def download(url, folder):
    os.makedirs(folder, exist_ok=True)
    command = f'cd {folder} && wget -q -c {url}'
    _, err = run_command(command, silent=False)
    if err != '':
        raise Exception(err)

def upload(filepath, bucket, key, region, endpoint):
    '''
    Requires the following env variables:
    $ export AWS_ACCESS_KEY_ID=MY_KEY
    $ export AWS_SECRET_ACCESS_KEY=MY_SECRET
    '''
    command = f'aws s3 cp {filepath} s3://{bucket}/{key} --region {region} --endpoint "{endpoint}"'
    _, err = run_command(command, silent=False)
    if err != '':
        raise Exception(err)

def mirror_single_file(url, bucket, region, endpoint, key):
    folder = 'data'
    download(url, folder)
    filename = url.split('/')[-1]
    filepath = f'{folder}/{filename}'
    upload(filepath, bucket, key, region, endpoint)
    os.remove(filepath)

def source_coop_tarballs():
    bucket = 'us-west-2.opendata.source.coop'
    region = 'us-west-2'
    endpoint = f'https://s3.{region}.amazonaws.com'

    r = requests.get('https://data.source.coop/mapterhorn/mapterhorn/attribution.json')
    mirror_items = json.loads(r.text)
    
    r = requests.get('https://download.mapterhorn.com/attribution.json')
    primary_items = json.loads(r.text)

    for primary_item in primary_items:
        should_mirror = True
        for mirror_item in mirror_items:
            if mirror_item['source'] == primary_item['source']:
                if mirror_item['tarball_md5sum'] == primary_item['tarball_md5sum']:
                    should_mirror = False
                break
        if should_mirror:
            print('mirror', primary_item['source'])
            url = f'https://download.mapterhorn.com/sources/{primary_item["source"]}.tar'
            key = f'mapterhorn/mapterhorn/sources/{primary_item["source"]}.tar'
            mirror_single_file(url, bucket, region, endpoint, key)

def source_coop_pmtiles():
    bucket = 'us-west-2.opendata.source.coop'
    region = 'us-west-2'
    endpoint = f'https://s3.{region}.amazonaws.com'

    r = requests.get('https://data.source.coop/mapterhorn/mapterhorn/download_urls.json')
    mirror_download_urls = json.loads(r.text)
    mirror_items = mirror_download_urls['items']
    
    r = requests.get('https://download.mapterhorn.com/download_urls.json')
    primary_download_urls = json.loads(r.text)
    primary_items = primary_download_urls['items']

    for primary_item in primary_items:
        should_mirror = True
        for mirror_item in mirror_items:
            if mirror_item['name'] == primary_item['name']:
                if mirror_item['md5sum'] == primary_item['md5sum']:
                    should_mirror = False
                break
        if should_mirror:
            print('mirror', primary_item['name'])
            url = f'https://download.mapterhorn.com/{primary_item["name"]}'
            key = f'mapterhorn/mapterhorn/{primary_item["name"]}'
            mirror_single_file(url, bucket, region, endpoint, key)

def source_coop_jsons():  
    bucket = 'us-west-2.opendata.source.coop'
    region = 'us-west-2'
    endpoint = f'https://s3.{region}.amazonaws.com'

    url = 'https://download.mapterhorn.com/attribution.json'
    key = 'mapterhorn/mapterhorn/attribution.json'
    mirror_single_file(url, bucket, region, endpoint, key)

    url = 'https://download.mapterhorn.com/download_urls.json'
    key = 'mapterhorn/mapterhorn/download_urls.json'
    mirror_single_file(url, bucket, region, endpoint, key)

if __name__ == '__main__':
    source_coop_tarballs()
    source_coop_pmtiles()
    source_coop_jsons()
