

# Mapterhorn Pipelines

Mapterhorn has four main pipelines that run in sequence: Source, Aggregation, Downsampling, and Bundle. The input is a set of tifs containing elevation data and the output are PMTiles files with terrain RGB (Terrarium-encoded WebP). This fork also merges public bathymetry into the same elevation surface using a shoreline land/ocean mask.

<img src="readme_imgs/pipeline.svg">

## Quick start (operator)

All commands below are run from `pipelines/`. Type `just` for the cheat sheet.

There are **two phases**. Several recipes overlap; you only need the ones in this path.

```bash
uv sync
just storage
just manage autodownload -y    # phase 1: sources + shoreline (skip complete, resume partial)
just covering                  # phase 2: plan tiles
# terminal A:
just downloader                # copy rasters into tmp-store as aggregate asks for them
# terminal B:
just aggregate
just downsample
just bundle VERSION=1
```

After sources are already on disk, `just all VERSION=1` runs covering through bundle (it starts the downloader in the background). It does **not** download sources.

`just status` / `just retry-failed` / `just preflight` are watch/recover tools, not extra stages.

Progress is written to `meta-store/run-status.json` and `meta-store/logs/{run_id}.log`. Failed items become `*.csv.failed` without aborting the whole worker pool (set `MAPTERHORN_ABORT_ON_WORKER_FAILURE=1` for legacy abort-all behavior).

### Just commands

| Command | What it actually does |
|---|---|
| `just storage` | Print which disk each store directory is on. |
| `just manage autodownload -y` | **The source command.** Download + unzip/bounds for every catalog source that has URLs, plus shoreline. Skips `DOWNLOAD_COMPLETE`. Resume partial files with wget. |
| `just manage list` | Table of catalog vs disk. `DL=yes` means the source is fully downloaded. |
| `just covering` | Read complete sources' `bounds.csv` and write the aggregation/downsampling work queues. |
| `just downloader` | Long-running loop: copy (or symlink) rasters from `source-store` into `tmp-store` as aggregate requests them. Run in its own terminal. |
| `just aggregate` | Merge staged rasters into terrain tiles. Needs the downloader running. |
| `just downsample` | Build lower zoom levels from aggregation output. |
| `just bundle VERSION=1` | Pack tiles into PMTiles + attribution/download URL files. |
| `just all VERSION=1` | covering + background downloader + aggregate + downsample + bundle. **Does not download sources.** |
| `just status` | Print `meta-store/run-status.json` (progress, ETA, failures). |
| `just retry-failed` | Turn `*.csv.failed` back into `*.todo`. |
| `just preflight` | Check GDAL/wget/disk/shoreline/at least one complete land source. |
| `just upload` | Push finished PMTiles (after bundle). |

Aliases you can ignore unless you need them:

| Command | Same as |
|---|---|
| `just shoreline` | Shoreline half of autodownload (`manage load-shoreline`) |
| `just sources SOURCES='gebco'` | `just manage load gebco` (default `SOURCES` is only `gebco`) |
| `just manage load NAME` | Autodownload one named source |
| `just manage reload NAME -y` | Delete that source, then download it again |
| `just manage clear NAME -y` | Delete only |
| `just manage mark-complete NAME` | After a manual FTP drop (UK England, Japan DEM, …) |

`just manage autodownload gebco -y` / `--ocean` / `--land` / `--dry-run` / `--force` limit or re-do autodownload.

### Bathymetry-specific steps

1. Shoreline mask is built by autodownload (or `just shoreline`).
2. Prepare ocean sources (`gebco`, `emodnet`, `bluetopo`, …) with `"domain": "ocean"` in their `metadata.json`.
3. Aggregation masks land vs ocean **after** each reproject and **before** the early-exit “fully filled” check, so land DEMs that encode ocean as `0` do not block bathymetry.
4. Web Mercator bounds are clamped to ±85.051° so polar GEBCO tiles do not explode `bounds.csv`.

Debug land+ocean smoke test:

```bash
bash debug.sh
```

## Source

The source pipeline has multiple parts that are needed to bring source files into a normalized file format.

`source_download.py`: Downloads files from URLs in `file_list.txt` to `source-store/{source}`. Writes `DOWNLOAD_COMPLETE` only after every URL succeeds. Interrupted runs leave that marker absent; `wget --continue` resumes partial files. If the marker is already present, the download is skipped.

`source_unzip.py`: If a source contains ZIP files, this script can be used to unpack them. Requires `DOWNLOAD_COMPLETE`.

`source_to_cog.py`: Use this script to make sure that all files are LERC compressed and tiled internally. Note that this is a bit of a mis-nomer because it does not actually create COGs since no overviews are added to the GeoTIFFs.

`source_fix_orientation.py`: Use this if there are y-axis issues in GDAL.

`source_set_crs.py`: Use this if the CRS is not well defined across all files. Note that per source there can only be a single CRS otherwise GDAL translate will complain in the aggregation_run.py stage.

`source_set_nodata.py`: Use this to set a NODATA value if it is missing.

`source_normalize_filenames.py`: Use this if you have strange filenames.

`source_prepare_shoreline.py`: Downloads S2Coast + GSHHG and builds `mask-store/shoreline/land_3857.gpkg`. Aggregation rasterizes these land polygons per tile to separate terrain from bathymetry.

`source_bathdnn_convert.py` / `source_bluetopo_extract.py` / `source_gmrt_download.py`: Bathymetry-specific ingest helpers.

`source_bounds.py`: Required script. Creates `source-store/{source}/bounds.csv` needed for the aggregation covering stage.

`source_polygonize.py`: Required script. Creates `polygon-store/{source}.gpkg` with the coverage polygon of the source. Needed for the tarball creation and the coverage pmtiles part.

`source_slice.py`: Use this if polygonize is very slow. This happens sometimes with large (>10 GB) tifs.

`source_remove_tifs.py`: Use this to delete the tifs from a `source-store/{source}` folder. The bounds.csv file will not be deleted.

`source_manage.py`: Clear, load, and autodownload source / shoreline data.

```bash
uv run python source_manage.py list
uv run python source_manage.py autodownload --yes          # all sources; skip complete; resume partial
uv run python source_manage.py autodownload gebco --yes
uv run python source_manage.py mark-complete ukengland     # after a manual FTP drop
uv run python source_manage.py clear gebco --yes
uv run python source_manage.py load gebco --yes            # skips already-complete sources
uv run python source_manage.py reload gebco --yes          # clear then load
uv run python source_manage.py reload --ocean --yes
uv run python source_manage.py clear-shoreline --yes
uv run python source_manage.py load-shoreline --force --yes
```

Also available as `just manage ...`. Complete downloads are marked with `source-store/{source}/DOWNLOAD_COMPLETE` (written only after every URL succeeds). Covering and the downloader ignore sources without that marker so half-downloaded data is never aggregated. Clear removes `source-store/{source}` plus polygon/tar/meta unless `--keep-derived`. Load/autodownload run the source's catalog Justfile.


`source_create_tarball.py`: Required script. Creates a tarball in `tar-store/{source}.tar`. Metadata is stored in `meta-store/tar/{source}.json`. Tarball will be needed in the upload stage.

`source_extract_tarball.py`: Extract tifs from a tarball in `tar-store/{source}.tar` to `source-store/{source}/`.

The `source-store/` folder should point to a folder on an SSD since access is random from multiple threads in the source and aggregation stages.

## Aggregation

The aggregation pipeline converts the source images to terrain RGB PMTiles files without overviews. All data is reprojected to web mercator, sources are merged with smooth edge blending, and the maxzoom is locally adjusted to fully resolve the source data.

The pipeline has two main parts. First, we plan what needs to be done. This part is called **covering**. Second, we execute the work. This part is called **run**.

In **covering**, we loop over all source bounds.csv files and all source files (or items) in the bounds file. We buffer the source item bounding box and compute which z12 tiles it intersects:

<img src="readme_imgs/source.svg">

These zoom 12 tiles are called "macrotiles". We then store in a map which macrotiles intersect which source items.

For every source item we furthermore compute the smallest web mercator zoom level to oversample the source data. Here is where we use the pixel size and bounding box from the bounds.csv file.

Throughout Mapterhorn we assume a final tile size of 512 by 512 pixels. Intermediate working tiles can also be larger but never smaller.

Once we have the macrotile to source item map, we group macrotiles by maxzoom and source. That is, if two macrotiles have source items with the same set of sources and maxzooms, they will be in the same group.

Now that every macrotile is assigned to a group we go ahead and turn macrotiles into what we call "aggregation tiles" by simplifying macrotiles of equal group:

<img src="readme_imgs/simplify.svg">

We limit how large aggregation tiles can be by requiring that their maxzoom to extent zoom difference is not more than 6. This means that an aggregation tile can be at most 64*512=32768 pixels wide. With float32 elevation data this yields roughly 4 gigabytes of uncompressed data.

The aggregation tiles are then written to aggregation csv files containing the work instructions, i.e., which source items to use and at what zoom level they should be reprojected. We store those in paths of the form `aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-aggregation.csv`

The aggregation_id is generated automatically each time the covering is executed. The aggregation tile extent is given by z, x, and y and child_z is the zoom level at which the source items should be sampled.

In the file we find a list of file names, sources, and maxzooms. Example `11-1078-718-17-aggregation.csv`:

```
source,filename,maxzoom
glo30,Copernicus_DSM_COG_10_N47_00_E009_00_DEM.tif,12
swissalti3d,swissalti3d_2019_2755-1227_0.5_2056_5728.tif,17
swissalti3d,swissalti3d_2019_2755-1230_0.5_2056_5728.tif,17
...
```

In **run**, we iterate over all aggregation items of the latest aggregation id and execute them.

If an item is identical to the corresponding one of the second-latest aggregation, then it can be skipped as nothing has changed since last time. Like this we can add, update, and remove sources without having to recompute the full planet each time.

Else we need to process the aggregation item. For this we first copy all relevant source image files from the source folder, which potentially is on a HDD, to a folder in the aggregation store, which we recommend is on an SSD because we need fast random access from multiple concurrent threads.

Then we group the source items by source and maxzoom, and order them such that higher maxzoom is more important than lower, and earlier lexicographic names are more important than later.

We iterate over the source item groups starting with the most important one and do the following:

1. Call gdal to make a virtual raster (vrt) of all source images
2. Call gdal to warp the vrt to web mercator
3. Call gdal to reproject the data
4. Check with rasterio if the resulting tif has nodata pixels. Break if not, else continue with the next source item group.

Now that we have reprojected the data to web mercator, we need to merge the tifs of different source item groups.

If there is only a single tif, nothing needs to be done.

If there are multiple tifs, we check the best one if it has no-data values. If so, we paint the second best into the best at the no-data value pixels. We also remember the seams of the no-data area,i.e. the pixel boundary between best and second-best. If there are still no-data values, we continue with adding pixels from the third-best u.s.w.

Once this is done, we have a full-filled tif which might contain data from multiple sources. Since sources in general will have different measurement values at a given pixel, there will be a jump in elevation at the source pixel boundarys. To make that jump a little less pronounced, we apply a gaussian blur along the pixel boundary line.

After having reprojected and merged the source data, we now have a tif that contains the aggregated data. What remains to be done in the aggregation pipeline is to store it as PMTiles. We use terrarium encoding since it has a finer resolution than mapbox encoding. Data is stored as webp RGB images which are  25 to 35 percent smaller than PNGs but they take longer to encode.

Tiles are optimized in size by limiting the vertical resolution depending on the zoom level. Terrarium has a maximal resolution of `1/256 m ~ 3.9 mm`. This is used at zoom level 19. At lower zoom levels, the vertical data is rounded to powers of 2 of this maximal resolution:

| z | Pixel Size 3857 | Vertical Resolution |
|----------|----------|----------|
| 0 | 78.3 km | 2048 m |
| 1 | 39.1 km | 1024 m |
| 2 | 19.6 km | 512 m |
| 3 | 9.78 km | 256 m |
| 4 | 4.89 km | 128 m |
| 5 | 2.45 km | 64 m |
| 6 | 1.22 km | 32 m |
| 7 | 611 m | 16 m |
| 8 | 306 m | 8 m |
| 9 | 153 m | 4 m |
| 10 | 76.4 m | 2 m |
| 11 | 38.2 m | 1 m |
| 12 | 19.1 m | 50 cm |
| 13 | 9.55 m | 25 cm |
| 14 | 4.78 m | 12.5 cm |
| 15 | 2.39 m | 6.3 cm |
| 16 | 1.19 m | 3.1 cm |
| 17 | 0.597 m | 1.6 cm |
| 18 | 0.299 m | 7.8 mm |
| 19 | 0.149 m | 3.9 mm |

As a consequence of the vertical rounding, the minimal angle between neighboring pixels is the same on all zoom levels and is given by `min_angle = atan(1 / 38.2) ~ 1.5 deg`. The pixel size in the above table is given in the projected Web Mercator coordinate system EPSG:3857.

We store the PMTiles data in the pmtiles-store folder using the same filename convention as the aggregation csv but just without the "-aggregation".

If the aggregation item has z &lt; 7, it is stored directly in the pmtiles-store folder. Else it is placed in a subfolder where the subfolder name is given by the zoom 7 parent of the aggregation item. Example: `pmtiles-store/7-67-44/12-2144-1434-17.pmtiles`

The `pmtiles-store/` can point to a folder on a HDD since access is sequential.

## Downsampling

The downsampling pipeline creates overviews from the aggregated PMTiles file which contain only data at the local maxzoom. The pipeline has again two parts: **covering** to plan the work, and **run** to execute the work.

In **covering**, we iterate over zoom levels starting with the highest and going lower down to zero. For a given zoom, we read all aggregation item extents and all previously produced downsampling extents, and simplify them again up to a total downsampling tile width of 64 * 512 = 32768 pixels. For each parent downsampling item we write which children are involved into a file at `aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-downsampling.csv`. 

Example content of `2-0-0-2-downsampling.csv`:

```
filename
3-1-1-3.pmtiles
3-0-1-3.pmtiles
3-1-0-3.pmtiles
```

In **run**, we iterate over all downsampling items in descending child zoom order and we first check if the involved aggregation items have changed since the last aggregation. If not, we can skip this item. Else process it as follows:

First we create a map from child tile id to pmtiles file by expanding the children of each file. Then, for each parent tile we get the 4 children to fill a 1024 by 1024 float32 array. We half the size to 512 by 512 using 2 by 2 averaging. The tiles are then encoded as terrarium again and written as webp to disk. Then we pack the webps into a pmtiles archive and store it in the pmtiles-store folder with the same file location convention as for aggregation items.


## Bundle

The last task is to bundle the single zoom level PMTiles files from aggregation and downsampling using the bundle pipeline.

The pmtiles-store folder contains thousands of files after aggregation and downsampling. They all have a single zoom level of tiles and they are at most 64 tiles wide, which means that their size can be at most around 1 gigabyte.

We now bundle these files by creating tile pyramids with multiple zoom levels. 

**planet.pmtiles** contains all tiles from zoom 0 to zoom 12.

**6-{x}-{y}.pmtiles** contains all zoom level 13+ children of tile 6-{x}-{y}.

## Requirements

- gdal: https://mothergeo-py.readthedocs.io/en/latest/development/how-to/gdal-ubuntu-pkg.html#install-gdal-ogr
- uv: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- just: https://github.com/casey/just?tab=readme-ov-file#installation
- aws cli: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- wget
- curl
- un7z
- unzip

## Hardware

The pipeline stages work well with **~2 GiB RAM per worker thread**. Example: 64 GiB RAM for a 32-core machine. Throughput rule of thumb: ~100 GiB of normalized input per hour on a 32-core box.

### Keep data outside the git repo

Do **not** symlink store folders into `pipelines/` (that fights git). Point all stores at a directory outside the checkout:

```bash
cp env.example .env
# edit .env:
#   MAPTERHORN_DATA_ROOT=/mnt/ssd/mapterhorn
#   MAPTERHORN_PMTILES_STORE=/mnt/hdd/mapterhorn/pmtiles-store   # optional
#   MAPTERHORN_BUNDLE_STORE=/mnt/hdd/mapterhorn/bundle-store
#   MAPTERHORN_TAR_STORE=/mnt/hdd/mapterhorn/tar-store

# just loads .env automatically; otherwise:
source .env   # or export vars in your shell profile
just storage
```

| Variable | Purpose |
|----------|---------|
| `MAPTERHORN_DATA_ROOT` | Base dir for every store (`$ROOT/source-store`, `$ROOT/tmp-store`, …) |
| `MAPTERHORN_PMTILES_STORE` etc. | Optional absolute override for one store (SSD/HDD split) |
| `MAPTERHORN_CATALOG_ROOT` | Rarely needed; defaults to `../source-catalog` next to `pipelines/` |

Store folders under `pipelines/` are **gitignored**. Prefer an empty `pipelines/` working tree plus `MAPTERHORN_DATA_ROOT` on your disks.

### What goes on SSD vs HDD

| Directory | Access pattern | Put on | Why |
|-----------|----------------|--------|-----|
| `source-store/` | Many random reads during prep + aggregation | **SSD** | Workers and GDAL hit many GeoTIFFs concurrently |
| `aggregation-store/` | Lots of small CSVs + markers | **SSD** | High metadata / small-file traffic |
| `tmp-store/` | Hot scratch (queue, copied sources, per-tile warps) | **SSD** | Fastest disk you have; size spikes during aggregate |
| `mask-store/` | Shoreline vectors + overview | SSD preferred | Modest size (~7 GB); read during every aggregation tile |
| `pmtiles-store/` | Large sequential writes/reads | **HDD** (or RAID0 HDDs) | Biggest intermediate output |
| `bundle-store/` | Final `planet.pmtiles` / `6-x-y.pmtiles` | **HDD** | Multi-TB distribution artifacts |
| `tar-store/` | Source tarballs for upload/archive | **HDD** | Cold-ish; not on the hot path |
| `polygon-store/`, `meta-store/`, `task-store/` | Small metadata | SSD or with `DATA_ROOT` | Tiny |

```
     MAPTERHORN_DATA_ROOT=/mnt/ssd/mapterhorn
                 │
                 ├─ source-store/        ┐
                 ├─ aggregation-store/   │ SSD (default under DATA_ROOT)
                 ├─ tmp-store/           │
                 └─ mask-store/          ┘
     MAPTERHORN_PMTILES_STORE=/mnt/hdd/.../pmtiles-store
     MAPTERHORN_BUNDLE_STORE=/mnt/hdd/.../bundle-store
     MAPTERHORN_TAR_STORE=/mnt/hdd/.../tar-store
```

If the SSD is large enough for sources **and** you set `MAPTERHORN_SOFTLINK_SOURCE=1`, the downloader can symlink from `source-store` into `tmp-store` instead of copying (saves SSD space and copy time). Default is copy (`0`).

### Mount the disks

```bash
sudo mkdir -p /mnt/ssd /mnt/hdd
sudo mount /dev/nvme0n1p1 /mnt/ssd    # example SSD
sudo mount /dev/sda1 /mnt/hdd         # example HDD

# Persist with UUIDs from `blkid` in /etc/fstab:
# UUID=....-ssd  /mnt/ssd  ext4  defaults,noatime  0  2
# UUID=....-hdd  /mnt/hdd  ext4  defaults,noatime  0  2

mkdir -p /mnt/ssd/mapterhorn /mnt/hdd/mapterhorn/{pmtiles-store,bundle-store,tar-store}
```

Then set `.env` as above. No symlinks inside the git tree.

### How big should each disk be?

| Disk | Bathymetry / regional experiment | Full planet (land + ocean) |
|------|----------------------------------|----------------------------|
| **SSD** | 200 GB–1 TB | **several TB** (`source-store` alone can be multi-TB; upstream cites ~14.5 TiB sources for the full land catalog) |
| **HDD** | 500 GB–2 TB | **10+ TiB** (`pmtiles-store` + bundles; published planet PMTiles ~10 TiB scale) |

On a constrained SSD you can still put individual huge sources on HDD via a per-source directory under an overridden layout, or keep `MAPTERHORN_SOFTLINK_SOURCE=0` so aggregation copies hot tiles into SSD `tmp-store` (capped by `MAPTERHORN_MAX_TMP_SOURCE_SIZE`, default **100** GiB).

### Environment knobs

| Variable | Default | Meaning |
|----------|---------|---------|
| `MAPTERHORN_DATA_ROOT` | cwd (`pipelines/`) | Where stores live (set this to leave git) |
| `MAPTERHORN_NUM_WORKERS` | 32 | Aggregation/downsampling worker processes |
| `MAPTERHORN_MAX_TMP_SOURCE_SIZE` | 100 | Max GiB of `tmp-store/source` before pruning |
| `MAPTERHORN_SOFTLINK_SOURCE` | 0 | `1` = symlink sources into tmp instead of copying |
| `MAPTERHORN_MIN_FREE_GB` | 50 | Preflight minimum free space |

### Check before a long run

```bash
just storage     # mount points + free space per store
just preflight
```

