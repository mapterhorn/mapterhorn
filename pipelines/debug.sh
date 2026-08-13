#!/bin/bash
# Small coastal/land+ocean debug run.
set -euo pipefail

uv run python source_prepare_shoreline.py

uv run python source_download.py debug-glo30
uv run python source_download.py debug-swissalti3d
uv run python source_download.py debug-gebco

uv run python source_normalize_filenames.py debug-gebco
uv run python source_bounds.py debug-glo30
uv run python source_bounds.py debug-swissalti3d
uv run python source_bounds.py debug-gebco

uv run python preflight.py

uv run python aggregation_covering.py
uv run python downloader.py &
DL_PID=$!
trap 'kill $DL_PID 2>/dev/null || true' EXIT
uv run python aggregation_run.py

uv run python downsampling_covering.py
uv run python downsampling_run.py

uv run python remove_dangling_pmtiles.py

TMPDIR=/tmp uv run python bundle.py 0.0.0

uv run python download_urls.py 0.0.0
uv run python attribution.py
uv run python status.py
