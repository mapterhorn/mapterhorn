import subprocess
from pathlib import Path
from glob import glob
import json
from datetime import datetime

def run_command(command):
    print(command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    
    print('stderr')
    print(stderr.decode())
    print('stdout')
    print(stdout.decode())

def create_local_store(path):
    folder_path = Path(path)
    folder_path.mkdir(parents=True, exist_ok=True)

def rsync(src, dst):
    command = f'rsync -avh {src} {dst}'
    run_command(command)

def get_collection_ids(source):
    '''
    returns collection ids ordered from oldest to newest
    '''
    paths = glob(f'cogify-store/3857/{source}/*')
    collection_ids = [path.split('/')[-1] for path in paths]
    timestamps = []
    for collection_id in collection_ids:
        with open(f'cogify-store/3857/{source}/{collection_id}/collection.json') as f:
            collection = json.load(f)
            time_string = collection['extent']['temporal']['interval'][0][0]
            time_string = time_string.replace('Z', '+00:00')
            timestamps.append(datetime.fromisoformat(time_string))

    return [sorted_id for _, sorted_id in sorted(zip(timestamps, collection_ids))]

def get_collection_items(source, collection_id):
    paths = glob(f'cogify-store/3857/{source}/{collection_id}/*.json')
    filenames = [path.split('/')[-1] for path in paths]
    return [item for item in filenames if item not in ['collection.json', 'covering.json', 'source.json']]