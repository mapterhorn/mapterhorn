# Mapterhorn Source Catalog

The Mapterhorn Source Catalog contains references to public open-data digital elevation models.

## Structure

Each subfolder defines a source where the folder name is at the same time the source name. The folder structure looks as follows:

```
source-catalog
├── README.md
├── glo30
│   ├── file_list.txt
│   ├── LICENSE.pdf
│   └── metadata.json
├── swissalti3d
│   ├── file_list.txt
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

## Adding a Source

Add a source by creating a new subfolder. The folder name will be the source name. Create the files `file_list.txt`, `LICENSE.pdf`, and `metadata.json`. 

Notes:
- Each file must have a unique name. 
- Only sources with open licenses are accepted.
- The metadata should include precise references to the producer.

## Updating a Source

Mapterhorn assumes that the contents of a file do not change as long as the filename stays the same. Source producers are expected to publish files with updated names if they publish new data.

For example for the swisstopo source image `2501-1120` there might be a version from the year 2019 called `swissalti3d_2019_2501-1120_0.5_2056_5728.tif` and a newer one from 2024 called `swissalti3d_2024_2501-1120_0.5_2056_5728.tif`.

If a source producer publishes updated data in new files with new filenames, remove the old URLs from `file_list.txt` and add the new ones.

## Removing a Source

Remove a source by removing its folder. Note that there might still be references to the deleted source in `pipelines/source-store`, so you might need to clean up there too.