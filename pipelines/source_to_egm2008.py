from glob import glob
import sys
import os
from multiprocessing import Pool

import numpy as np
import rasterio
from rasterio.windows import Window
from scipy.ndimage import map_coordinates
import pyproj
from pyproj import CRS, Transformer

import utils

# pyproj ships its own PROJ data folder, which does not contain the EGM2008 geoid
# grid us_nga_egm08_25.tif; point it at the system grid so the vertical shift is
# actually applied. The grid must be resident locally (see check_geoid_grid).
PROJ_DIRS = [pyproj.datadir.get_user_data_dir(), '/usr/share/proj',
             '/usr/local/share/proj', '/opt/homebrew/share/proj']
for _p in PROJ_DIRS:
    if os.path.isfile(os.path.join(_p, 'us_nga_egm08_25.tif')):
        pyproj.datadir.append_data_dir(_p)
        break

NCOARSE = 512   # geoid sample points per axis across the tile (~100 m spacing
                # on a 50 km tile). The undulation is smooth (the EGM2008 grid
                # is 4.6 km native), so bilinear upsampling from this lattice
                # matches a dense per-pixel transform to sub-mm almost
                # everywhere, a few mm at worst.
STRIP = 1024    # rows per strip: two 512-px block rows; bounds the
                # interpolation buffers

def coarse_delta(crs, transform, width, height):
    # The height to add per pixel to shift ellipsoidal heights to EGM2008
    # (-N, the negated geoid undulation), sampled at NCOARSE x NCOARSE points
    # spanning the tile. The shift depends only on horizontal position and is
    # smooth, so to_egm2008 upsamples this instead of evaluating PROJ per pixel.
    # Note: A per-pixel evaluation (gdalwarp to a compound +3855 CRS) computes
    # the same heights but costs minutes per mosaic tile.
    src = CRS.from_user_input(crs.to_wkt())
    cols = np.linspace(0, width - 1, NCOARSE)
    rows = np.linspace(0, height - 1, NCOARSE)
    cc, rr = np.meshgrid(cols + 0.5, rows + 0.5)
    x = transform.c + cc * transform.a + rr * transform.b
    y = transform.f + cc * transform.d + rr * transform.e
    lon, lat = Transformer.from_crs(src, 'EPSG:4326', always_xy=True).transform(x, y)
    # A direct crs -> crs+3855 transform would no-op the height (the 2D source
    # CRS has no vertical axis), so the shift goes through geographic coordinates.
    _, _, delta = Transformer.from_crs('EPSG:4979', 'EPSG:4326+3855', always_xy=True).transform(lon, lat, np.zeros_like(lon))
    return np.asarray(delta, dtype=np.float64)

def check_geoid_grid(crs, transform, width, height):
    # PROJ silently falls back to a no-op vertical transform when the geoid grid
    # is missing, so verify up front that the shift is actually non-zero.
    delta = coarse_delta(crs, transform, width, height)
    if not np.isfinite(delta).all() or np.nanmax(np.abs(delta)) < 1e-6:
        print('The EGM2008 geoid grid us_nga_egm08_25.tif does not seem to be available,')
        print('the heights would not be shifted. Install the proj-data package, or')
        print('download https://cdn.proj.org/us_nga_egm08_25.tif into one of the')
        print(f'folders this script checks: {", ".join(PROJ_DIRS)}.')
        print('Do not use PROJ_NETWORK=ON as a substitute:')
        print('the grid must be resident locally, because a network failure later in')
        print('the run can leave heights unshifted without an error.')
        exit(1)
    print(f'geoid grid check ok, max undulation over the test tile is {np.nanmax(np.abs(delta)):.3f} m...')

def to_egm2008(filepath):
    # Shifts one file's heights from ellipsoidal to EGM2008 in place. The
    # pixel grid, CRS, and nodata are untouched.
    with rasterio.open(filepath) as src:
        crs = src.crs
        transform = src.transform
        width, height = src.width, src.height
        nodata = src.nodata
        profile = src.profile.copy()
    delta_coarse = coarse_delta(crs, transform, width, height)
    tmp = f'{filepath}.tmp'
    profile.pop('compress', None)  # uncompressed temporary: LERC quantizes the heights once, in the translate
    profile.update(driver='GTiff', dtype='float32', tiled=True, blockxsize=512,
                   blockysize=512, SPARSE_OK='YES', BIGTIFF='YES')
    col_index = np.arange(width) * (NCOARSE - 1) / (width - 1)
    with rasterio.open(filepath) as src, rasterio.open(tmp, 'w', **profile) as dst:
        for row0 in range(0, height, STRIP):
            rows = min(STRIP, height - row0)
            band = src.read(1, window=Window(0, row0, width, rows))
            row_index = np.arange(row0, row0 + rows) * (NCOARSE - 1) / (height - 1)
            ri, ci = np.meshgrid(row_index, col_index, indexing='ij')
            delta = map_coordinates(delta_coarse, [ri, ci], order=1, mode='nearest')
            shifted = band.astype(np.float32) + delta.astype(np.float32)
            if nodata is not None:
                shifted[band == nodata] = nodata
            dst.write(shifted, 1, window=Window(0, row0, width, rows))
    # The marker is metadata, not a compound CRS on the file. A compound CRS
    # would make any later warp to a 2D CRS (like the web mercator warp in
    # the aggregation stage) shift the heights back.
    out, err = utils.run_command(f'GDAL_CACHEMAX=512 gdal_translate -mo VERTICAL_DATUM=EGM2008 -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001 "{tmp}" "{tmp}.cog"', silent=True)
    if 'ERROR' in err or not os.path.isfile(f'{tmp}.cog'):
        utils.run_command(f'rm -f "{tmp}" "{tmp}.cog"', silent=True)
        raise ValueError(f'gdal_translate failed for {filepath}, original left in place: {err}')
    os.remove(tmp)
    os.replace(f'{tmp}.cog', filepath)

def main():
    if len(sys.argv) != 2:
        print('source argument missing...')
        return
    source = sys.argv[1]
    print(f'converting {source} from ellipsoidal heights to EGM2008...')

    filepaths = sorted(glob(f'source-store/{source}/*.tif'))
    todo = []
    already_converted = 0
    for filepath in filepaths:
        with rasterio.open(filepath) as src:
            if src.crs is None:
                raise ValueError(f'crs not defined on {filepath}')
            if src.tags().get('VERTICAL_DATUM') == 'EGM2008' or 'EGM2008' in src.crs.to_wkt():
                already_converted += 1
                continue
            todo.append(filepath)

    if already_converted > 0:
        print(f'skipping {already_converted} files which already have EGM2008 heights...')
    if len(todo) == 0:
        print('nothing to do...')
        return

    with rasterio.open(todo[0]) as src:
        check_geoid_grid(src.crs, src.transform, src.width, src.height)

    with Pool() as pool:
        for _ in pool.imap_unordered(to_egm2008, todo):
            pass

if __name__ == '__main__':
    main()
