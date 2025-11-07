# itsicily

A WMS service was scraped with a script that looks roughly like the one below. Important to note that the tile size had to below a certain limit - 512 worked fine, 4100 did not - because otherwise the server would crash in the middle of the island while it would still work on the shore. So it looks like the limit the number of DATA pixels...

```python
import math
import os
import requests
from urllib.parse import urlencode

SERVICE_URL = 'https://map.sitr.regione.sicilia.it/gis/rest/services/modelli_digitali/mdt_2013/ImageServer'
OUT_DIR = 'sicily_dtm_2013_tiles'
TILE_SIZE_PX = 512
OUTPUT_FORMAT = 'tiff'
SR = 25833

def get_service_metadata():
    meta_url = f'{SERVICE_URL}?f=json'
    resp = requests.get(meta_url)
    resp.raise_for_status()
    return resp.json()

def make_bbox(xmin, ymin, xmax, ymax):
    return f'{xmin},{ymin},{xmax},{ymax}'

def download_tile(tile_id, bbox):
    params = {
        'f': 'image',
        'bbox': bbox,
        'bboxSR': SR,
        'size': f'{TILE_SIZE_PX},{TILE_SIZE_PX}',
        'imageSR': SR,
        'format': OUTPUT_FORMAT,
        'pixelType': 'F32',
        'interpolation': 'RSP_Bilinear'
    }

    url = f'{SERVICE_URL}/exportImage?' + urlencode(params)
    out_path = os.path.join(OUT_DIR, f'tile_{tile_id}.tif')
    print(url)
    print(f'Downloading {tile_id}: {bbox}')
    r = requests.get(url, stream=True)
    r.raise_for_status()

    with open(out_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f'Saved {out_path}')

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    meta = get_service_metadata()
    full_extent = meta['extent']

    xmin, ymin = full_extent['xmin'], full_extent['ymin']
    xmax, ymax = full_extent['xmax'], full_extent['ymax']

    cell_size_x = meta['pixelSizeX']
    cell_size_y = meta['pixelSizeY']

    tile_w = TILE_SIZE_PX * cell_size_x
    tile_h = TILE_SIZE_PX * cell_size_y

    n_cols = math.ceil((xmax - xmin) / tile_w)
    n_rows = math.ceil((ymax - ymin) / tile_h)

    tile_id = 0
    for row in range(n_rows):
        for col in range(n_cols):
            x0 = xmin + col * tile_w
            x1 = min(x0 + tile_w, xmax)
            y1 = ymax - row * tile_h
            y0 = max(y1 - tile_h, ymin)
            bbox = make_bbox(x0, y0, x1, y1)
            tile_id += 1
            try:
                download_tile(tile_id, bbox)
            except Exception as e:
                print(f'Failed tile {tile_id}: {e}')

if __name__ == '__main__':
    main()
```