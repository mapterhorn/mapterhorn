# Build tiled GMRT GridServer download URLs covering the ocean.
# GMRT GridServer rejects very large requests, so we tile in 20x20 degree cells.

BASE = (
    "https://www.gmrt.org/services/GridServer"
    "?minlongitude={minlon}&maxlongitude={maxlon}"
    "&minlatitude={minlat}&maxlatitude={maxlat}"
    "&format=geotiff&resolution=med&layer=topo"
)

def main():
    step = 20
    urls = []
    for minlat in range(-80, 80, step):
        for minlon in range(-180, 180, step):
            maxlat = minlat + step
            maxlon = minlon + step
            url = BASE.format(
                minlon=minlon, maxlon=maxlon, minlat=minlat, maxlat=maxlat
            )
            # GridServer does not set Content-Disposition filenames; encode in query
            # source_download uses URL basename; add a fake path segment via download name
            # We rely on source_gmrt_download.py for naming; keep URL list here.
            urls.append(url)
    with open("file_list.txt", "w") as f:
        for url in urls:
            f.write(url + "\n")
    print("wrote {} tile URLs".format(len(urls)))

if __name__ == "__main__":
    main()
