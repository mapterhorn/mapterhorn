from glob import glob
import sys
import tarfile

def main():
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'creating tarball for {source}...')
    else:
        print('Not enough arguments. Usage: source_create_tarball.py {{source}}')
        exit()
    
    with tarfile.open(f'tar-store/{source}.tar', 'w') as tar:
        tar.add(f'../source-catalog/{source}/LICENSE.pdf', 'LICENSE.pdf')
        tar.add(f'../source-catalog/{source}/metadata.json', 'metadata.json')
        tar.add(f'source-store/{source}/bounds.csv', 'bounds.csv')
        tar.add(f'polygon-store/{source}.gpkg', 'coverage.gpkg')
        filepaths = glob(f'source-store/{source}/*.tif')
        for j, filepath in enumerate(filepaths, 1):
            if j % 1000 == 0:
                print(f'{j:_} / {len(filepaths):_}')
            filename = filepath.split('/')[-1]
            tar.add(filepath, f'files/{filename}')

if __name__ == '__main__':
    main()

