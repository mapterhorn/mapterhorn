#!/bin/bash

pixi run python source_download.py debug-glo30
pixi run python source_download.py debug-swissalti3d
pixi run python source_bounds.py debug-glo30
pixi run python source_bounds.py debug-swissalti3d

pixi run python aggregation_covering.py
pixi run python aggregation_run.py

pixi run python downsampling_covering.py
pixi run python downsampling_run.py

pixi run python remove_dangling_pmtiles.py

TMPDIR=/tmp pixi run python bundle.py 0.0.0

pixi run python download_urls.py 0.0.0
pixi run python attribution.py
