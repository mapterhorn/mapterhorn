# nl50cm


The Dutch geospatial agency distributes DEM data like surface (DSM) and terrain models (DTM) in the AHN (Algemeen Hoogtebestand Nederland) 
series. In the DSM and DTM elevation models water bodies are filled with NODATA pixels. 
In the terrain model (DTM), buildings and vegetation are removed and also filled with NODATA pixels.
Two resolutions are available: 5m and 0.5m (50cm).
This folder applies to the high-resolution 50cm per pixel version of AHN. The folder [nl5m](../nl5m) applies to 5m AHN resolution.

## Versions

Versions of AHN evolve through the years. Each version gets a subsequent number AHN1, AHN2, ...AHN6. 
Within a given year, dependent on flights, who depend on (clear) weather, a given version may
not cover the whole of The Netherlands. For example, in 2026, about half (mostly the Western part) is covered by AHN5,
the other part by AHN6. As their coverages hardly overlap, these sets are combined.

## Data Download

Via https://www.ahn.nl/dataroom metadata information is provided via OGC webservices like WFS and OGC API Features.
In particular the OGC API Feature service per version provides easy access to the list of URLs:

* AHN5: https://api.ellipsis-drive.com/v3/ogc/features/65945b69-81df-4270-97f0-f029033154c1/
* AHN6: https://api.ellipsis-drive.com/v3/ogc/features/0820faae-5240-499b-8486-cf406433cf71/

The script [get-file-list.sh](get-file-list.sh) will fetch the [available grids](https://basisdata.nl/hwh-ahn/AUX/bladwijzer/index.html) 
with URLs and processes these to create the downloadable files for the AHN-version and resolution of DTMs in [file_list.txt](file_list.txt).
For testing a small list can be used: [file_list_test.txt](file_list_test.txt)

## Additional Processing

* fill NODATA* using [source_fill_nodata.py](../../pipelines/source_fill_nodata.py). Calls [gdal_fillnodata.py](https://gdal.org/en/stable/programs/gdal_fillnodata.html).
* assign Dutch CRS EPSG:28992 after polygonizing to GPKG coverage polygons

TODO*: NODATA is filled per source-TIFF. If the result is suboptimal, try with a mosaic of all TIFFs, built with `gdalbuildvrt`.
