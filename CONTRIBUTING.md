# Contributing

Contributions to Mapterhorn are welcome.

## Questions and Ideas

If you have any questions or ideas, feel free to open a GitHub Discussion or a GitHub Issue.

## Bugs

If you find a bug please open an Issue.

## New Data Sources

If you want to add a new data source to Mapterhorn, please follow these step:

1. Check the license of your new data first. Mapterhorn accepts a wide range of licenses and governments will often have special  licenses that are related to local open-data legislation. As a general rule, anything that is similar or not more restrictive than CC-BY-4.0 will be accepted. However, if the source data license forbids the use in commercial settings or has a share-alike clause, you cannot add it to Mapterhorn. Likewise, if your data does not have a license at all it will not be added.
1. Make a new source in the source-catalog folder. This folder has its own README with steps about how to add a source.
1. Create a source tarball and run the aggregation, downsampling, and bundling pipelines to create some PMTiles files of just your new source. Make the tarball and the PMTiles available for download on http storage of your choosing.
1. Create a pull request with your new source folder in the source-catalog folder and share the links to your source tarball and the PMTiles.

If you get stuck anywhere along these steps feel free to reach out.
