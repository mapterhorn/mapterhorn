# Mapterhorn Source Catalog

The Mapterhorn Source Catalog contains references to public open-data digital elevation models.

## Structure

Each subfolder defines a source where the folder name is at the same time the source name. The folder structure looks as follows:

```
source-catalog
├── README.md
├── glo30
│   ├── file_list.txt
│   ├── Justfile
│   ├── LICENSE.pdf
│   └── metadata.json
├── swissalti3d
│   ├── file_list.txt
│   ├── Justfile
│   ├── LICENSE.pdf
│   └── metadata.json
...    
```

### `file_list.txt`

Contains the download URLs of available source images. One image per line. We assume that the filenames are unique across a given source.

Example `swissalti3d/file_list.txt`:

```
https://data.geo.admin.ch/ch.swisstopo.swissalti3d/swissalti3d_2019_2501-1120/swissalti3d_2019_2501-1120_0.5_2056_5728.tif
https://data.geo.admin.ch/ch.swisstopo.swissalti3d/swissalti3d_2019_2501-1121/swissalti3d_2019_2501-1121_0.5_2056_5728.tif
https://data.geo.admin.ch/ch.swisstopo.swissalti3d/swissalti3d_2019_2501-1122/swissalti3d_2019_2501-1122_0.5_2056_5728.tif
...
```

### `Justfile`

Contains instructions about which scripts to use to download and normalize the source data. Should be run from the `mapterhorn/pipelines` folder.

Example `desachsenanhalt/Justfile`:

```
# Source preparation pipeline to be run from mapterhorn/pipelines folder
[no-cd]
default:
    uv run python source_download.py desachsenanhalt
    uv run python source_unzip.py desachsenanhalt
    uv run python source_set_crs.py desachsenanhalt EPSG:25832
    uv run python source_bounds.py desachsenanhalt
    uv run python source_polygonize.py desachsenanhalt 32
    uv run python source_create_tarball.py desachsenanhalt
```

You would run this Justfile from the pipelines folder with `just ../source-catalog/desachsenanhalt/`.

### `LICENSE.pdf`

Contains a copy of the original source license in PDF format. This can be a printout of a website listing the original license.

### `metadata.json`

Contains information about the source data producer and the source license.

Example `swissalti3d/metadata.json`:

```
{
    "name": "swissALTI3D",
    "website": "https://www.swisstopo.admin.ch/en/height-model-swissalti3d",
    "license": "Open Government Data",
    "producer": "Federal Office of Topography swisstopo",
    "resolution": 0.5,
    "access_year": 2025
}
```

### Optional `domain` field

`metadata.json` may include `"domain": "land" | "ocean" | "both" | "mask"`.

| domain | Meaning |
|--------|---------|
| `land` (default) | Terrain DEM. Ocean pixels with elevation `>= 0` are treated as nodata so bathymetry can fill them. Negative coastal topobathy pixels are kept. |
| `ocean` | Bathymetry. Land pixels are masked to nodata using the shoreline product. |
| `both` | True topobathy compilations; no shoreline masking. |
| `mask` | Not an elevation source (e.g. `s2coast`). Prepared via `source_prepare_shoreline.py` into `mask-store/` (vector land polygons rasterized per aggregation tile). |

### Land / ocean merge

Aggregation uses a shoreline mask (S2Coast-2023 + GSHHG Antarctica) so land DEMs and ocean bathymetry do not overwrite each other. After each source group is warped to Web Mercator, the domain rule above is applied, then existing nodata-fill + seam blending runs. Higher native resolution still wins via `maxzoom`.

**Known limitation:** vertical datums differ across products (EGM/MSL vs LAT vs NAVD88). Coastal seams may show small vertical offsets; homogenization is out of scope for v1.

### Bathymetry sources in this catalog

| Source ID | Product | Notes |
|-----------|---------|-------|
| `gebco` | GEBCO 2026 | Global ~450 m baseline ocean fill |
| `bathdnn` | BathDNN25 | Coarser SWOT DNN model; fills only where finer sources are absent |
| `emodnet` | EMODnet DTM 2024 | European seas ~115 m |
| `bluetopo` | NOAA BlueTopo | US waters, multi-resolution |
| `gmrt` | GMRT synthesis | Sparse high-res measured bathymetry |
| `nonna` | CHS NONNA | Canada (file list may need refresh) |
| `ausseabed` | AusSeabed | Australia (add URLs from catalogue) |
| `linzbathy` | LINZ | New Zealand (add URLs from LINZ Data Service) |
| `s2coast` | Shoreline mask | Not elevation; run `just shoreline` from `pipelines/` |

## Adding a Source

Add a source by creating a new subfolder. The folder name will be the source name. Create the files `file_list.txt`, `Justfile`, `LICENSE.pdf`, and `metadata.json`. 

Notes:
- Each file must have a unique name. 
- Licenses which are share-alike or which do not allow commercial usage will not be accepted.
- The metadata should include precise references to the producer.
- Individual GeoTiffs in the source should not be larger than say 10 GB, otherwise the polygonize step gets slow. You can slice a larger tif into multiple smaller ones with `source_slice.py`.
- For bathymetry, set `"domain": "ocean"`.
- Lines in `file_list.txt` starting with `#` are ignored.

## Updating a Source

Mapterhorn assumes that the contents of a file do not change as long as the filename stays the same. Source producers are expected to publish files with updated names if they publish new data.

For example for the swisstopo source image `2501-1120` there might be a version from the year 2019 called `swissalti3d_2019_2501-1120_0.5_2056_5728.tif` and a newer one from 2024 called `swissalti3d_2024_2501-1120_0.5_2056_5728.tif`.

If a source producer publishes updated data in new files with new filenames, remove the old URLs from `file_list.txt` and add the new ones.

## Removing a Source

Remove a source by removing its folder. Note that there might still be references to the deleted source in `pipelines/source-store`, so you might need to clean up there too.