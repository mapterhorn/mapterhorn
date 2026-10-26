#!/bin/bash

pixi run --script source_download.py debug-glo30
pixi run --script source_download.py debug-swissalti3d
pixi run --script source_bounds.py debug-glo30
pixi run --script source_bounds.py debug-swissalti3d

pixi run --script aggregation_covering.py
pixi run --script aggregation_run.py

pixi run --script downsampling_covering.py
pixi run --script downsampling_run.py

pixi run --script remove_dangling_pmtiles.py

TMPDIR=/tmp pixi run --script bundle.py 0.0.0

pixi run --script download_urls.py 0.0.0
pixi run --script attribution.py
