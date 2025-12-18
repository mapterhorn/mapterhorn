import re
import requests

def main():
    r = requests.get('https://nedlasting.geonorge.no/geonorge/ATOM/hoydedata/datasett/DTM1.atom')
    pattern = r'https://nedlasting\.geonorge\.no/hoydedata/DTM1/[^/"]+-[^/"]+-[^/"]+\.tif'
    urls = re.findall(pattern, r.text)
    with open('file_list.txt', 'w') as f:
        for url in urls:
            f.write(f'{url}\n')

if __name__ == '__main__':
    main()