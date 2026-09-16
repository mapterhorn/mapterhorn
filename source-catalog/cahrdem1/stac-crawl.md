The **stac-crawl.py** helper utility makes it easy to discover and list specific data sources from multiple Canadian elevation collections (DTM, DSM, hillshade variants, etc.).

**Quick Start:**
```bash
python stac-crawl.py > file_list.txt  # Export the 1m DTM file list
```

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
