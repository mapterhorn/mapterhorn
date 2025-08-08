_default:
  @just --list --unsorted

# Download source from internet
download source:
    cd pipelines && uv run python source_download.py {{source}}

# Download debug sources
[group('debug')]
download-debug-sources: (download "debug-glo30") (download "debug-swissalti3d")

# Create bounds for source
bounds source:
    cd pipelines && uv run python source_bounds.py {{source}}

# Create bounds for debug sources
[group('debug')]
bounds-debug-sources: (bounds "debug-glo30") (bounds "debug-swissalti3d")

# Aggregation covering
aggregation-covering:
    cd pipelines && uv run python aggregation_covering.py

# Downsampling covering
downsampling-covering:
    cd pipelines && uv run python downsampling_covering.py

# Run debug pipeline
pipeline: download-debug-sources bounds-debug-sources aggregation-covering downsampling-covering
