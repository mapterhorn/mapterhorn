from glob import glob

"""Generate attribution.json manifest listing all sources used in current bundle.

This module ensures proper attribution and license compliance by:

**Attribution collection**:
1. Scan current aggregation CSVs to identify used sources
2. Load metadata.json from source-catalog/ for each source:
   - name: Human-readable source name
   - producer: Organization that created the data
   - website: Official data source URL
   - license: SPDX identifier (e.g., CC-BY-4.0, ODC-By)
   - license_pdf: Link to LICENSE.pdf in source-catalog repo
   - resolution: Native pixel spacing (e.g., "1/3 arc-second")
   - access_year: When data was downloaded
3. Add tarball metadata:
   - tarball_size: Bytes (for download estimates)
   - tarball_md5sum: Checksum from .md5 sidecar
   - tarball_url: CDN download link
4. Sort by source name and write bundle-store/attribution.json

**Tarball purpose**:
Source tarballs contain original raw DEM files as downloaded from
producers, enabling:
- License compliance (redistributing original data as required)
- Reproducibility (others can verify pipeline inputs)
- Transparency (users see exactly what sources contributed)

**Output format**:
```json
[
  {
    "source": "usgs-1-3-arc-second",
    "name": "USGS 1/3 Arc-Second DEM",
    "producer": "U.S. Geological Survey",
    "website": "https://...",
    "license": "CC0-1.0",
    "license_pdf": "https://github.com/.../LICENSE.pdf",
    "resolution": "1/3 arc-second",
    "access_year": 2024,
    "tarball_url": "https://download.mapterhorn.com/sources/usgs-1-3-arc-second.tar",
    "tarball_size": 123456789,
    "tarball_md5sum": "abc123..."
  }
]
```

**Legal compliance**: This manifest enables end users to:
- Identify data sources and their licenses
- Access original data tarballs for verification
- Comply with attribution requirements (e.g., CC-BY licenses)
- Understand data provenance and quality metrics
"""

import json
import os

import utils


def main():
    """Build bundle-store/attribution.json from source metadata and tarballs.

    Assembly process:
    1. Load latest aggregation ID (most recent ULID)
    2. Scan all aggregation CSVs:
       a. get_grouped_source_items: Parse CSV rows
       b. Extract unique source names from all CSVs
       c. Build set (deduplicates): {'usgs-1-3-arc-second', 'glo-30', ...}
    3. For each source:
       a. Load ../source-catalog/{source}/metadata.json
       b. Extract: name, website, license, producer, resolution, access_year
       c. Build license_pdf URL from GitHub repo path
       d. Stat tar-store/{source}.tar for size
       e. Read tar-store/{source}.tar.md5 for checksum
       f. Build tarball_url for CDN download
       g. Assemble item dict with all fields
    4. Sort items alphabetically by source name
    5. Write as JSON array to bundle-store/attribution.json

    Error handling:
    - Exits if tar file missing (prevents incomplete attributions)
    - Assumes metadata.json has all required fields (validated earlier)

    The output JSON is uploaded to CDN alongside PMTiles bundles,
    enabling clients to display attribution and access source data.

    Output:
        bundle-store/attribution.json: Complete attribution manifest
    """
    aggregation_id = utils.get_aggregation_ids()[-1]
    filepaths = glob(f"aggregation-store/{aggregation_id}/*-aggregation.csv")

    sources = set({})
    for filepath in filepaths:
        grouped_source_items = utils.get_grouped_source_items(filepath)
        for source_items in grouped_source_items:
            for source_item in source_items:
                sources.add(source_item["source"])

    sources = sorted(list(sources))
    data = []
    for source in sources:
        item = None
        with open(f"../source-catalog/{source}/metadata.json") as f:
            metadata = json.load(f)
            item = {
                "source": source,
                "name": metadata["name"],
                "website": metadata["website"],
                "license": metadata["license"],
                "producer": metadata["producer"],
                "license_pdf": f"https://github.com/mapterhorn/mapterhorn/blob/main/source-catalog/{source}/LICENSE.pdf",
                "resolution": metadata["resolution"],
                "access_year": metadata["access_year"],
            }
        tar_filepath = f"tar-store/{source}.tar"
        if not os.path.isfile(tar_filepath):
            print("Error: tar file missing for source {source}")
            return
        item["tarball_size"] = os.path.getsize(tar_filepath)
        with open(f"{tar_filepath}.md5") as f:
            line = f.readline()
            item["tarball_md5sum"] = line.strip().split(" ")[0]
        item["tarball_url"] = f"https://download.mapterhorn.com/sources/{source}.tar"
        data.append(item)

    with open("bundle-store/attribution.json", "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()
