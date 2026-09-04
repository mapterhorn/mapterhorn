import utils
from glob import glob
import os

def main():
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]
    dirty_filepaths = set([filepath.replace('.todo', '') for filepath in glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv.todo')])
    all_sources = set({})
    dirty_sources = set({})

    filepaths = glob(f'aggregation-store/{aggregation_id}/*-aggregation.csv')
    for filepath in filepaths:
        with open(filepath, 'r') as f:
            for line in f.readlines()[1:]:
                parts = line.split(',')
                all_sources.add(parts[0])
                if filepath in dirty_filepaths:
                    dirty_sources.add(parts[0])
    
    tifs_missing = []
    for source in sorted(list(all_sources)):
        print(source)
        if source in dirty_sources:
            print('  dirty: YES')
            num_tif_filepaths = len(glob(f'source-store/{source}/*.tif'))
            if num_tif_filepaths == 0:
                print('  tif count ZERO')
                print('adding to list\n...')
                tifs_missing.append(source)
            else:
                print(f'  tif count {num_tif_filepaths}')
        else:
            print('  dirty NO')

        if os.path.isfile(f'polygon-store/{source}.gpkg'):
            print('  polygon OK')
        else:
            print('  polygon MISSING')
            print('\naborting...')
            return
        
        if os.path.isfile(f'source-store/{source}/bounds.csv'):
            print('  bounds OK')
        else:
            print('  bounds MISSING')
            print('\naborting...')
            return
        print()

    if len(tifs_missing) > 0:
        print('sources with missing tifs:\n')
        for source in tifs_missing:
            print(source)
    else:
        print('everything looks good.')

if __name__ == '__main__':
    main()