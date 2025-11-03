# nlahn5lowresfilled

The Dutch geospatial agency distributes surface and terrain models in the AHN serie. In both types of elevation models, water bodies are filled with NODATA pixels. In the terrain model, buildings and vegetation are removed and also filled with NODATA pixels.

Here we download the low-resolution 5 m per pixel version of AHN5.

We then created a mosaic with `gdalbuildvrt` and filled NODATA pixels by interpolation with `gdal_fillnodata.py` with default settings.
