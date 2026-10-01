import utils
import sys
import os

def download_from_internet(source, token_env_var=None):
    urls = []
    with open(f'../source-catalog/{source}/file_list.txt') as f:
        urls = [l.strip() for l in f.readlines()]

    token = None
    if token_env_var:
        token = os.environ.get(token_env_var)
        if token:
            print(f'Using bearer token from environment variable: {token_env_var}')
        else:
            print(f'Warning: Environment variable {token_env_var} not found, downloading without token')

    j = 0
    for url in urls:
        j += 1
        if j % 100 == 0:
            print(f'downloaded {j} / {len(urls)}')

        auth_header = f'--header="Authorization: Bearer {token}" ' if token else ''
        command = f'cd source-store/{source} && wget {auth_header}--progress=bar:force --continue "{url}"'
        utils.run_command(command, silent=False)

def main():
    source = None
    token_env_var = None

    if len(sys.argv) > 1:
        source = sys.argv[1]
        print(f'downloading {source}...')
    else:
        print('source argument missing...')
        exit()

    if len(sys.argv) > 2:
        token_env_var = sys.argv[2]

    utils.create_folder( f'source-store/{source}/')
    download_from_internet(source, token_env_var)

if __name__ == '__main__':
    main()
