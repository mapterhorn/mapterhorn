import sys
import os
from multiprocessing import Pool
import shutil

import utils

SILENT = False

def polygonize_tif(source, filename):
    mask_filepath = f'polygon-store/{source}/{filename}'
    utils.run_command(f'GDAL_CACHEMAX=1024 gdal_calc.py -A source-store/{source}/{filename} --outfile={mask_filepath} --calc="A*0+1" --type=Byte --overwrite', silent=SILENT)
    utils.run_command(f'GDAL_CACHEMAX=1024 gdal_polygonize.py {mask_filepath} -b 1 -f "GPKG" polygon-store/{source}/{filename}.gpkg -overwrite', silent=SILENT)
    os.remove(mask_filepath)

def get_filenames(source):
    lines = None
    with open(f'source-store/{source}/bounds.csv') as f:
        lines = f.readlines()
    lines = [l.strip() for l in lines[1:]]
    filenames = [line.split(',')[0] for line in lines]
    return filenames

def polygonize_source(source, processes):
    filenames = get_filenames(source)
    utils.create_folder(f'polygon-store/{source}/')
    argument_tuples = []
    for filename in filenames:
        argument_tuples.append((source, filename))
    with Pool(processes) as pool:
        pool.starmap(polygonize_tif, argument_tuples, chunksize=1)

def merge_source(source):
    filenames = get_filenames(source)
    merged_filepath = f'polygon-store/{source}/merged.gpkg'
    if os.path.isfile(merged_filepath):
        os.remove(merged_filepath)
    command = f'ogr2ogr -f GPKG {merged_filepath} polygon-store/{source}/{filenames[0]}.gpkg'
    utils.run_command(command, silent=False)
    for j, filename in enumerate(filenames[1:]):
        if j % 100 == 0:
            print(f'{j:_} / {len(filenames):_}')
        command = f'ogr2ogr -f GPKG -update -append {merged_filepath} polygon-store/{source}/{filename}.gpkg -nln out -append -addfields'
        utils.run_command(command, silent=True)
    union_filepath = f'polygon-store/{source}.gpkg'
    if os.path.isfile(union_filepath):
        os.remove(union_filepath)
    utils.run_command(f'ogr2ogr -f GPKG {union_filepath} {merged_filepath} -nln union -dialect sqlite -sql "SELECT ST_Union(ST_MakeValid(geom)) AS geom FROM out"', silent=False)

def main():
    source = None
    processes = None
    if len(sys.argv) == 3:
        source = sys.argv[1]
        processes = int(sys.argv[2])
        print(f'polygonizing {source} with {processes} processes...')
    else:
        print('Not enough arguments. Usage: source_polygonize.py {{source}} {{processes}}')
        exit()
    polygonize_source(source, processes)
    merge_source(source)
    shutil.rmtree(f'polygon-store/{source}')

if __name__ == '__main__':
    main()

