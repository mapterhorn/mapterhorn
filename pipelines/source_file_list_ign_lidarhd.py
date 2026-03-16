from glob import glob
import sys
import urllib.parse

BASE_URL = "https://data.geopf.fr/wms-r"


def parse_tile_name(tile_name):
    parts = tile_name.strip().split('_')
    if len(parts) < 4:
        raise ValueError(f"invalid tile name: {tile_name}")
    return int(parts[2]), int(parts[3])


def calculate_bbox(x_km, y_km):
    min_x = x_km * 1000 - 0.25
    max_x = (x_km + 1) * 1000 - 0.25
    min_y = (y_km - 1) * 1000 + 0.25
    max_y = y_km * 1000 + 0.25
    return f"{min_x},{min_y},{max_x},{max_y}"


def build_url(bbox, output_filename):
    params = {
        "SERVICE": "WMS",
        "VERSION": "1.3.0",
        "EXCEPTIONS": "text/xml",
        "REQUEST": "GetMap",
        "LAYERS": "IGNF_LIDAR-HD_MNT_ELEVATION.ELEVATIONGRIDCOVERAGE.LAMB93",
        "FORMAT": "image/geotiff",
        "STYLES": "",
        "CRS": "EPSG:2154",
        "BBOX": bbox,
        "WIDTH": "2000",
        "HEIGHT": "2000",
        "FILENAME": output_filename,
    }
    return BASE_URL + "?" + urllib.parse.urlencode(params)


def main():
    if len(sys.argv) != 2:
        print('Usage: python source_file_list_ign_lidarhd.py {source}')
        exit()

    source = sys.argv[1]
    tile_list_paths = sorted(glob(f'../source-catalog/{source}/tile_list/*.txt'))
    if len(tile_list_paths) == 0:
        raise ValueError(f'no tile lists found for source {source}')

    urls = []
    seen_urls = set({})
    for tile_list_path in tile_list_paths:
        with open(tile_list_path) as f:
            tile_names = [line.strip() for line in f.readlines() if line.strip()]
        for tile_name in tile_names:
            x_km, y_km = parse_tile_name(tile_name)
            output_filename = f'{tile_name}_MNT_O_0M50_LAMB93_IGN69.tif'
            url = build_url(calculate_bbox(x_km, y_km), output_filename)
            if url not in seen_urls:
                urls.append(url)
                seen_urls.add(url)

    with open(f'../source-catalog/{source}/file_list.txt', 'w') as f:
        for url in urls:
            f.write(f'{url}\n')

    print(f'wrote {len(urls)} urls for {source}')


if __name__ == '__main__':
    main()
