import subprocess
from pathlib import Path

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
