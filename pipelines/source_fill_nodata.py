from glob import glob
import sys

import utils

from multiprocessing import Pool

SILENT = False

# Replaces NoData pixels in a raster with interpolated values computed from neighbooring valid pixels using gdal_fillnodata.py.
# It uses an inverse-distance weighted average of pixels found around the edge of each gap, followed by optional smoothing passes.
# This is the standard tool for closing voids in DEMs (classic SRTM gaps), patching missing scanlines in satellite imagery,
# or cleaning up mask-induced holes after a processing step.
# From: https://gdal.org/en/stable/programs/gdal_fillnodata.html
#
# Common commandline options
# -b <band>: The specific raster band number to process (default is 1)
# -md <pixels>: Maximum search distance to find valid neighbor pixels (default is 100)
# -si <iterations>: Number of 3 × 3 smoothing filter passes to run after filling to reduce edges (default is 0).
# Example: gdal_fillnodata.py -b 1 -si 2 -md 90 mydem.tif
# Args are passed here as follows: source_fill_nodata.py source [band] [max_dist] [smooth_iters]
# Defaults same as in gdal_fillnodata.py.
#
# Author: Just van den Broecke - justb4

def fillnodata(filepath, band, max_dist, smooth_iters):
    utils.run_command(f'mv "{filepath}" "{filepath}.bak"', silent=SILENT)
    utils.run_command(f'gdal_fillnodata.py -b {band} -md {max_dist} -si {smooth_iters} "{filepath}.bak" "{filepath}"', silent=SILENT)
    utils.run_command(f'rm "{filepath}.bak"', silent=SILENT)

def main():
    source = ''
    band = 1
    max_dist = 100
    smooth_iters = 2

    if len(sys.argv) >= 2:
        source = sys.argv[1]
    elif len(sys.argv) >= 3:
        band = sys.argv[2]
    elif len(sys.argv) >= 4:
        max_dist = sys.argv[3]
    elif len(sys.argv) >= 5:
        max_dist = sys.argv[4]
    else:
        print('wrong number of arguments: source_fill_nodata.py source [band] [max_dist] [smooth_iters]')
        exit()
    
    filepaths = sorted(glob(f'source-store/{source}/*.tif'))

    argument_tuples = []
    for filepath in filepaths:
        argument_tuples.append((filepath, band, max_dist, smooth_iters))

    print(f'fillnodata for  {source}: -b {band} -md {max_dist} -si {smooth_iters}')
    with Pool() as pool:
        pool.starmap(fillnodata, argument_tuples, chunksize=1)

if __name__ == '__main__':
    main()
