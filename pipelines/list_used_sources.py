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
    
    for source in sorted(list(all_sources)):
        print(source)
        if source in dirty_sources:
            print('  dirty: YES')
            num_tif_filepaths = len(glob(f'source-store/{source}/*.tif'))
            if num_tif_filepaths == 0:
                print('  tif count ZERO')
                print('\naborting...')
                exit()
            else:
                print(f'  tif count {num_tif_filepaths}')
        else:
            print('  dirty NO')

        if os.path.isfile(f'polygon-store/{source}.gpkg'):
            print('  polygon OK')
        else:
            print('  polygon MISSING')
            print('\naborting...')
            exit()
        
        if os.path.isfile(f'source-store/{source}/bounds.csv'):
            print('  bounds OK')
        else:
            print('  bounds MISSING')
            print('\naborting...')
            exit()
        print()


if __name__ == '__main__':
    main()