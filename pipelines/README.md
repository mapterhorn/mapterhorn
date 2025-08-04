# Mapterhorn Pipelines

## Steps

File urls are listed in `../source-catalog/{source}/file_list.txt`.

To get started work with debug sources `debug-glo30` and `debug-swissalti3d`.

### Source

Download files to `source-store` folder:

```bash
uv run python source_download.py debug-glo30
uv run python source_download.py debug-swissalti3d
```

Calculate web mercator bounds, store CRS and pixel size in `source-store/{source}/bounds.csv`

```bash
uv run python source_bounds.py debug-glo30
uv run python source_bounds.py debug-swissalti3d
```

This will create files like this:

```
source-store
├── debug-glo30
│   ├── bounds.csv
│   ├── Copernicus_DSM_COG_10_N47_00_E006_00_DEM.tif
│   ├── Copernicus_DSM_COG_10_N47_00_E007_00_DEM.tif
│   └── Copernicus_DSM_COG_10_N47_00_E008_00_DEM.tif
└── debug-swissalti3d
    ├── bounds.csv
    ├── swissalti3d_2019_2621-1249_0.5_2056_5728.tif
    ├── swissalti3d_2019_2621-1250_0.5_2056_5728.tif
    ...
```


### Aggregation

First, create an aggregation covering. Each time you run this you get a new folder inside `aggregation-store/` with the aggregation id.

```bash
uv run python aggregation_covering.py
```

This will create files like this:

```
aggregation-store/01K1TDNM7AMC3BB1VKT96J9CTT/
├── 10-532-356-12-aggregation.csv
├── 10-532-357-12-aggregation.csv
├── 10-532-358-12-aggregation.csv
...
```

In this example, `01K1TDNM7AMC3BB1VKT96J9CTT` is the aggregation id and `10-532-356-12-aggregation.csv` is an aggregation item filename. The filename name composes of `{z}-{x}-{y}-{child_z}-aggregation.csv` where `child_z` is the zoom level of the tiles and `z,x,y` define the extent tile of the aggregation item.

The aggregation csv lists which files from which sources are needed to create the aggregation item. Example `12-2135-1432-17-aggregation.csv`:

```csv
source,filename,crs,maxzoom
debug-glo30,Copernicus_DSM_COG_10_N47_00_E007_00_DEM.tif,EPSG:4326,12
debug-swissalti3d,swissalti3d_2019_2621-1256_0.5_2056_5728.tif,EPSG:2056,17
debug-swissalti3d,swissalti3d_2019_2621-1257_0.5_2056_5728.tif,EPSG:2056,17
debug-swissalti3d,swissalti3d_2019_2621-1258_0.5_2056_5728.tif,EPSG:2056,17
...
```

Second, run the aggregation. This will reproject, merge, and tile the data:

```
uv run python aggregation_run.py
```

You should now have some `.pmtiles` files in the `pmtiles-store` folder:

```
pmtiles-store
├── 7-66-44
│   ├── 10-532-356-12.pmtiles
│   ├── 10-532-357-12.pmtiles
    ...
```

If the extent fits into a zoom 7 tile, it will be put in a folder starting with `pmtiles-store/7-`. The folder `7-66-44` for example contains all children of `z=7, x=66, y=44` pmtiles. Else, it will be put directly into the `pmtile-store/` folder. This is such that data can be distributed to multiple block storage devices. At a maxzoom of for example 17, a zoom 7 subpyramid will be roughly one terabyte in size.

### Downsampling

The aggregation pmtiles files contain a single zoom level, the `child_z` zoom level. We now need to downsample the data first by creating a covering:

```bash
uv run python downsampling_covering.py
```

The aggregation store should now contain some downsampling csv files:

```
aggregation-store/01K1TDNM7AMC3BB1VKT96J9CTT
...
├── 11-1068-716-12-downsampling.csv
├── 11-1068-716-13-downsampling.csv
├── 11-1068-716-14-downsampling.csv
├── 11-1068-716-15-downsampling.csv
├── 11-1068-716-16-downsampling.csv
├── 11-1068-716-17-aggregation.csv
...
```

They contain the data relevant for the downsampling item. Example `11-1068-716-16-downsampling.csv`:

```
filename
11-1068-716-17.pmtiles
```

Second, downsample with:

```bash
uv run python downsampling_create.py
```

### Remove Dangling

The previous commands for aggregation and downsampling detect which is the minimal amount of work when a new source is added. Only the tiles for the affected aggregation and downsampling items will be created. 

In turn, we need to clean the pmtiles store from danging pmtiles files with:

```bash
uv run python remove_dangling_pmtiles.py
```

### Create Index

```bash
uv run python create_index.py
```

### Preview Data
```
npx serve . --cors
```