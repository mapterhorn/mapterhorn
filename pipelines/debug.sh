#!/bin/bash

uv run python source_download.py debug-glo30
uv run python source_download.py debug-swissalti3d
uv run python source_bounds.py debug-glo30
uv run python source_bounds.py debug-swissalti3d

uv run python aggregation_covering.py
uv run python aggregation_run.py

uv run python downsampling_covering.py
uv run python downsampling_run.py

uv run python remove_dangling_pmtiles.py

TMPDIR=/tmp uv run python bundle.py
