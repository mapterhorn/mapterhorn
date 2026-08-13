# AusSeabed compilation file list helper.
# Prefer the AusSeabed portal / AWS open data listings.
# Operators should refresh this list from the current AusSeabed catalogue.

NOTE = """# AusSeabed bathymetry compilations (CC BY 4.0)
# Discover current GeoTIFF products at https://www.ausseabed.gov.au/
# and https://nationalmap.gov.au/ or AusSeabed AWS open data.
# Paste one HTTPS GeoTIFF URL per line below.
"""

def main():
    with open("file_list.txt", "w") as f:
        f.write(NOTE)
    print("wrote stub file_list.txt — add product URLs from AusSeabed catalogue")

if __name__ == "__main__":
    main()
