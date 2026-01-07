# ca

## Overview

This directory contains Canadian elevation data sourcing for the [Mapterhorn](https://github.com/mapterhorn/mapterhorn/) project. It provides access to high-resolution Digital Surface Models (DSM) and related elevation datasets from Canada's official STAC catalog, this source is the 1m hrdsm-mosaic collection (~7.7TB).

> **Note:** The combined Elevation Model for Canada is over 100 TB in size.

The **stac-crawl.py** utility found in `../ca-hrdem-01m` makes it easy to discover and list specific data sources from multiple Canadian elevation collections (DTM, DSM, hillshade variants, etc.).

