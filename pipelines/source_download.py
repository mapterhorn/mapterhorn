import local_config
import utils

def download_from_internet():
    urls = []
    with open(f'../source-catalog/{local_config.source}/file_list.txt') as f:
        line = f.readline().strip()
        while line != '':
            urls.append(line)
            line = f.readline().strip()
    for url in urls:
        filename = url.split('/')[-1]
        command = f'wget --no-verbose -O source-store/{local_config.source}/{filename} -c {url}'
        utils.run_command(command, silent=False)

if __name__ == '__main__':
    remote = f'{local_config.remote_source_store_path}/{local_config.source}/'
    local = f'source-store/{local_config.source}/'

    utils.create_folder(local)

    utils.rsync(src=remote, dst=local)

    download_from_internet()

    utils.rsync(src=local, dst=remote)
