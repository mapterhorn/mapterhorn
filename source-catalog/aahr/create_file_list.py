from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen
import xml.etree.ElementTree as ET


FEED_URL = "https://geoportal.dgu.hr/services/atom/el-cov/xml"


def main():
    with urlopen(FEED_URL, timeout=30) as response:
        feed = ET.parse(response)

    urls = [
        link.attrib["href"]
        for link in feed.findall(".//{http://www.w3.org/2005/Atom}link")
        if link.get("type") == "image/tiff"
    ]
    if not urls:
        raise ValueError("The elevation feed contains no GeoTIFF download links")

    filenames = [Path(urlsplit(url).path).name for url in urls]
    if len(set(filenames)) != len(filenames):
        raise ValueError("The elevation feed contains duplicate GeoTIFF filenames")

    output = Path(__file__).with_name("file_list.txt")
    output.write_text("".join(f"{url}\n" for url in urls), encoding="utf-8")
    print(f"Wrote {len(urls)} GeoTIFF URLs to {output}")


if __name__ == "__main__":
    main()
