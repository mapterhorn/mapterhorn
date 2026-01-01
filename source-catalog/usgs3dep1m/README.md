# usgs3dem1m

## Overview

This directory contains United States elevation data sourcing for the [Mapterhorn](https://github.com/mapterhorn/mapterhorn/) project. It provides access to high-resolution 1-meter 3D Digital Elevation Models (DEMs) from the USGS 3D Elevation Program (3DEP). Two complementary utilities are provided:

- **wms-crawl.py** - Query the ScienceBase metadata catalog to discover and list all 1-meter GeoTIFF elevation tiles
- **http-crawl.py** - Query the AWS S3 bucket directly to enumerate all TIFF files in cloud storage

> **Note:** The 1m DEM collection for the US contains over 110,000 tiles, with a total size greater than 24TB.

**Quick Start:**
```bash
python wms-crawl.py > file_list.txt         # Use ScienceBase catalog API
python http-crawl.py > file_list_s3.txt     # Use AWS S3 listing
```

## Features

### Both Crawlers
- 📊 **List download resources** - Enumerate all TIFF files from the collection
- 📋 **Multiple output modes** - Simple URI-only view or detailed view with comprehensive metadata
- 📈 **Summary statistics** - View file counts and total size
- ⚡ **Performance optimized** - Configurable request delays to respect rate limits

### wms-crawl.py (ScienceBase)
- 🔍 **Metadata discovery** - Query the official ScienceBase catalog API
- 📋 **Detailed metadata** - Access item titles, descriptions, and modification dates
- ✅ **Verified inventory** - Get authoritative metadata from USGS

### http-crawl.py (AWS S3)
- 🪣 **Direct S3 access** - Query AWS S3 bucket listing without credentials
- ⚡ **Faster enumeration** - Direct object storage listing (no metadata overhead)
- 🔗 **S3 native URIs** - Direct download URLs from cloud storage

## Data Source

- **Collection:** USGS 1 Meter Digital Elevation Models (DEMs) - 3DEP Downloadable Data
- **Location:** https://www.sciencebase.gov/catalog/item/543e6b86e4b0fd76af69cf4c
- **Format:** GeoTIFF
- **Resolution:** 1 meter
- **Coverage:** Conterminous United States and Alaska
- **Data Authority:** U.S. Geological Survey (USGS)

## Data Details

The 1m Digital Elevation Models (DEMs) are produced through the 3D Elevation Program (3DEP). Key characteristics:

- **Bare Earth Surface:** Elevations represent the topographic bare-earth surface
- **Source:** High-resolution lidar data (≥1m resolution)
- **Seamlessness:** Seamless within collection projects; may not be seamless across projects
- **Vertical Reference:** North American Vertical Datum of 1988 (NAVD88)
- **Horizontal Reference:** Universal Transverse Mercator (UTM) in NAD83
- **Units:** Meters
- **Delivery:** Tiled by UTM zone (some tiles delivered in both zones if crossing boundaries)

## Usage

### wms-crawl.py - ScienceBase Catalog API

#### List all 1m DEM TIFF files from ScienceBase

```bash
python wms-crawl.py
```

Outputs a simple list of download URIs, one per line, suitable for piping to `wget` or `curl`.

#### View detailed information with metadata

```bash
python wms-crawl.py --detail
```

Shows detailed information including filenames, item titles, and file sizes.

#### Include summary statistics

```bash
python wms-crawl.py --summary
```

Adds a summary section showing total file count and combined size.

#### Combine options

```bash
python wms-crawl.py --detail --summary
```

#### Limit processing for testing

```bash
python wms-crawl.py --max-items 100
```

Useful for testing or previewing a subset of the data.

#### Export file list

```bash
python wms-crawl.py > file_list.txt
python wms-crawl.py --detail --summary > file_list_detailed.txt
```

### http-crawl.py - AWS S3 Direct Listing

#### List all 1m DEM TIFF files from AWS S3

```bash
python http-crawl.py
```

Outputs a simple list of S3 download URIs, one per line, directly from the cloud bucket.

#### View detailed S3 information

```bash
python http-crawl.py --detail
```

Shows detailed information including filenames, S3 key paths, and file sizes.

#### Include summary statistics

```bash
python http-crawl.py --summary
```

Adds summary showing total file count and combined size.

#### Combine options

```bash
python http-crawl.py --detail --summary
```

#### Limit processing for testing

```bash
python http-crawl.py --max-items 100
```

#### Crawl specific S3 prefix

```bash
python http-crawl.py --prefix "StagedProducts/Elevation/1m/Projects/ID_AdamsCounty_2019_B19/"
```

Query a specific project folder or custom prefix.

#### Export file list from S3

```bash
python http-crawl.py > file_list_s3.txt
python http-crawl.py --detail --summary > file_list_s3_detailed.txt
```

### Comparing the Two Approaches

| Feature | wms-crawl.py | http-crawl.py |
|---------|--------------|---------------|
| Source | ScienceBase API | AWS S3 API |
| Metadata | Rich (titles, descriptions) | Minimal (key, size) |
| Speed | Moderate (per-item fetches) | Fast (bulk listing) |
| Authentication | None required | None required |
| Use Case | Discovery, metadata-rich output | Fast enumeration, direct S3 access |

## Command-Line Options

### wms-crawl.py

```
usage: wms-crawl.py [-h] [--detail] [--summary] [--max-items MAX_ITEMS] [--delay DELAY]

Crawl USGS 1m DEM collection and list GeoTIFF download URIs

optional arguments:
  -h, --help            show this help message and exit
  --detail              Show detailed information for each TIFF
  --summary             Show summary statistics
  --max-items MAX_ITEMS
                        Maximum number of items to process (default: all)
  --delay DELAY         Delay in seconds between API requests (default: 0.2)
```

### http-crawl.py

```
usage: http-crawl.py [-h] [--detail] [--summary] [--max-items MAX_ITEMS] [--delay DELAY] [--prefix PREFIX]

Crawl AWS S3 bucket and list USGS 1m DEM GeoTIFF download URIs

optional arguments:
  -h, --help            show this help message and exit
  --detail              Show detailed information for each TIFF
  --summary             Show summary statistics
  --max-items MAX_ITEMS
                        Maximum number of items to process (default: all)
  --delay DELAY         Delay in seconds between API requests (default: 0.2)
  --prefix PREFIX       S3 prefix to crawl (default: StagedProducts/Elevation/1m/Projects/)
```

## Requirements

- Python 3.6+
- `requests` library

Install dependencies:
```bash
pip install requests
```

## Implementation Notes

### wms-crawl.py (ScienceBase)

Uses the ScienceBase API to:
1. Query the parent item (`543e6b86e4b0fd76af69cf4c`) to get a list of child items
2. For each child item, fetch full details to extract TIFF download links
3. Parse download URIs from the webLinks array
4. Display or aggregate results based on user options

The ScienceBase list endpoint provides pagination with `offset` and `limit` parameters, allowing efficient retrieval of large collections without loading everything into memory at once.

### http-crawl.py (AWS S3)

Uses the AWS S3 REST API to:
1. Query the S3 bucket with a specific prefix to list objects
2. Parse XML response containing object metadata (key, size)
3. Filter for TIFF files (.tif, .tiff extensions)
4. Construct direct S3 download URIs for each file
5. Display or aggregate results based on user options

The S3 List API provides pagination with `marker` and `max-keys` parameters for handling large result sets. No AWS credentials are required since the bucket has public listing enabled.

## Data Access

All downloads are sourced from AWS S3:
- Bucket: `prd-tnm.s3.amazonaws.com`
- Prefix: `StagedProducts/Elevation/1m/Projects/`

Files can be downloaded directly using standard tools:
```bash
wget https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/...
curl -O https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/...
```

## Related Resources

- [USGS 3D Elevation Program](https://www.usgs.gov/3d-elevation-program)
- [National Elevation Dataset (NED)](https://www.usgs.gov/core-science-systems/ngp/ss/3dep-product-metadata)
- [The National Map Viewer](https://nationalmap.gov/elevation.html)
- [ScienceBase Catalog](https://www.sciencebase.gov/catalog/)

## License

The USGS 3DEP data is in the public domain. See the [USGS Data and Tools](https://www.usgs.gov/faqs/what-public-domain) page for more information.
