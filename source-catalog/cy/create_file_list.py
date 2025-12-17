import re
import requests

def main():
    r = requests.get('https://eservices.dls.moi.gov.cy/inspire_downloads/EL/rasters/2019_DTM/')
    pattern = r'ETRS89_\d+_\d+\.tif(?=</A>)'
    filenames = re.findall(pattern, r.text)
    with open('file_list.txt', 'w') as f:
        for filename in filenames:
            f.write(f'https://eservices.dls.moi.gov.cy/inspire_downloads/EL/rasters/2019_DTM/{filename}\n')

if __name__ == '__main__':
    main()