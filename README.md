# Mapterhorn (bathymetry)

Public terrain **and** bathymetry tiles for interactive web map visualizations. This workspace extends [Mapterhorn](https://github.com/mapterhorn/mapterhorn) so the same Terrarium PMTiles surface can show seafloor depths as well as land elevations.

## What changed vs upstream

- **Shoreline masking** — S2Coast-2023 + GSHHG Antarctica decide land vs ocean so land DEMs (often ocean=`0`) do not block bathymetry.
- **Source `domain`** — `land` (default), `ocean`, `both`, or `mask` in `source-catalog/*/metadata.json`.
- **Bathymetry catalog** — GEBCO 2026, BathDNN25, EMODnet, NOAA BlueTopo, GMRT, plus helpers for NONNA / AusSeabed / LINZ.
- **Unattended ops** — `just status`, heartbeats in `meta-store/run-status.json`, `.failed` items with `just retry-failed`, and `just preflight`.

## Repository layout

| Path | Role |
|------|------|
| [source-catalog/](source-catalog/) | Per-source download lists, metadata, Justfiles |
| [pipelines/](pipelines/) | Download → aggregate → downsample → bundle |
| [website/](website/) | Static site (viewer, coverage, attribution) |

## Run the pipeline

```bash
cd pipelines
uv sync
just preflight
just shoreline
just manage list
just manage reload gebco -y    # clear stale source-store data and re-prep
just sources SOURCES="glo30 gebco emodnet"
just covering
# separate terminals:
just downloader
just aggregate
just downsample
just bundle VERSION=1
just status
```

See [pipelines/README.md](pipelines/README.md) for stage details, hardware notes, and bathymetry behavior. See [source-catalog/README.md](source-catalog/README.md) for how to add sources and the land/ocean merge rules.

## Requirements

- GDAL (`gdalwarp`, `gdal_translate`, `gdal_rasterize`, `ogr2ogr`, …)
- [uv](https://github.com/astral-sh/uv), [just](https://github.com/casey/just), wget
- **SSD** for `source-store/`, `aggregation-store/`, `tmp-store/` (~2 GiB RAM per worker thread)
- **HDD** for `pmtiles-store/`, `bundle-store/`, `tar-store/` (large sequential output)

Set `MAPTERHORN_DATA_ROOT` (see [`pipelines/env.example`](pipelines/env.example)) so all stores live **outside the git checkout** — no in-repo symlinks. Check with `just storage`. Details in [pipelines/README.md](pipelines/README.md) → Hardware.

## License notes

Catalog policy is unchanged: commercial-OK, no share-alike. OSM coastlines (ODbL) are not used; S2Coast (CC BY 4.0) is the primary shoreline. Always verify per-product attribution in each source's `LICENSE.pdf` / `metadata.json`.
