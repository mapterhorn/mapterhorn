# LINZ NZ bathymetry file list helper.
# LINZ layers change; query the LINZ Data Service or open data catalogue.

NOTE = """# LINZ bathymetry (CC BY 4.0)
# Browse https://data.linz.govt.nz/ for bathymetry / hydrographic DEM layers
# and paste direct GeoTIFF download URLs below (one per line).
"""

def main():
    with open("file_list.txt", "w") as f:
        f.write(NOTE)
    print("wrote stub file_list.txt — add LINZ GeoTIFF URLs")

if __name__ == "__main__":
    main()
