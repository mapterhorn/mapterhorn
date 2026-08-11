# nlahn5lowresfilled

The Dutch geospatial agency distributes surface and terrain models in the AHN serie. In both types of elevation models, water bodies are filled with NODATA pixels. In the terrain model, buildings and vegetation are removed and also filled with NODATA pixels.

Here we download the low-resolution 5 m per pixel version of AHN5.

See https://basisdata.nl/hwh-ahn/AUX/bladwijzer/index.html?

* generate a file list using get-file-list.sh from OGC API Features metadata
* assign Dutch CRS EPSG:28992 to GPKG polygons 
* fill NODATA (or maybe: create mosaic with `gdalbuildvrt` and fill NODATA pixels by interpolation with `gdal_fillnodata.py`!)

