# ca

## Overview

This directory contains Canadian elevation data sourcing for the [Mapterhorn](https://github.com/mapterhorn/mapterhorn/) project. It provides access to high-resolution Digital Terrain Models (DTM) and related elevation datasets from Canada's official STAC catalog, defaulting to the 1m hrdem-mosaic collection (~7.3TB). The **stac-crawl.py** utility makes it easy to discover and list specific data sources from multiple Canadian elevation collections (DTM, DSM, hillshade variants, etc.).

> **Note:** The 1m Digital Terrain Model (DTM) for Canada is ~7.3TB in size.

**Quick Start:**
```bash
python stac-crawl.py > file_list.txt  # Export the 1m DTM file list
```

## Features

- 📊 **List catalog resources** - Browse collections and view available assets with detailed metadata
- 🔍 **Filter assets** - Filter by asset type (DTM, DSM, hillshade variants, etc.)
- 📋 **Multiple output modes** - Simple URI-only view or detailed view with comprehensive metadata
- 📈 **Summary statistics** - View file counts, total size, and modification dates
- 🗺️ **Discover collections** - Explore all available collections and their asset types


#### Basic Usage

List all DTM (Digital Terrain Model) files from the default `hrdem-mosaic-1m` collection:

```bash
python stac-crawl.py
```

#### List a Different Asset Type

```bash
python stac-crawl.py dsm
python stac-crawl.py hillshade
```

#### Browse a Different Collection

```bash
python stac-crawl.py collection_id asset_type
```

Example:
```bash
python stac-crawl.py hrdem-mosaic-2m 
```

#### View Detailed Information

Show detailed metadata including title, description, roles, and modification dates:

```bash
python stac-crawl.py --detail
python stac-crawl.py dsm --detail
```

#### Show Summary Statistics

Display summary statistics (file count, total size, newest file date):

```bash
python stac-crawl.py --summary
python stac-crawl.py dtm --detail --summary
```

#### Explore Collections

Display a matrix of all available collections and their asset types:

```bash
python stac-crawl.py --explore
```

### Examples

```bash
# List all DTM files from hrdem-mosaic-1m (default)
python stac-crawl.py

# List DSM files with details
python stac-crawl.py dsm --detail

# List all DTM files with summary
python stac-crawl.py --summary

# Full detail with summary statistics
python stac-crawl.py dtm --detail --summary

# Discover what collections and assets are available
python stac-crawl.py --explore

# Browse a different collection
python stac-crawl.py hrdem-mosaic-2m hillshade-dsm
```

### Output Format

#### Simple Mode (default)
Displays only the S3 URIs of the files, one per line.

#### Detail Mode (--detail)
Shows comprehensive metadata in columns:
- URI - S3 path to the file
- Asset Name - Name of the asset in the STAC metadata
- Title - Human-readable title
- Description - Asset description
- Roles - Asset roles in the collection
- Size - File size
- Modified - Last modified date

#### Summary (--summary)
Appends summary statistics:
- Total Files - Number of resources found
- Combined Size - Total size of all files
- Newest Modified Date - Most recent modification date

### How It Works

1. Connects to the STAC API at https://datacube.services.geo.ca/stac/api/
2. Queries the specified collection's items endpoint
3. Extracts GeoTIFF assets matching the filter
4. Retrieves file sizes using HTTP HEAD requests to S3
5. Displays results in the requested format

### Notes

- Default collection: `hrdem-mosaic-1m` (Canadian 1m elevation mosaic)
- Default asset filter: `dtm` (Digital Terrain Model)
- File sizes are retrieved via HTTP HEAD requests for efficiency
- Includes automatic pagination handling for large result sets