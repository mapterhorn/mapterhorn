from glob import glob
import sys
import subprocess
from multiprocessing import Pool

def run_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout.decode(), stderr.decode()

def fix_orientation(filepath):
    run_command(f'mv {filepath} {filepath}.bak')
    run_command(f'gdalwarp -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{filepath}.bak" "{filepath}"', silent=True)
    run_command(f'rm {filepath}.bak')

def main(source):
    filepaths = sorted(glob(f'source-store/{source}/*'))

    argument_tuples = []
    for filepath in filepaths:
        if not filepath.endswith('.tif'):
            continue
        argument_tuples.append((filepath,))
    
    with Pool() as pool:
        pool.starmap(fix_orientation, argument_tuples, chunksize=1)

if __name__ == '__main__':
    source = None
    if len(sys.argv) == 2:
        source = sys.argv[1]
        print(f'fixing orientation for source {source}...')
    else:
        print('source argument missing...')
        exit()
    main(source)
