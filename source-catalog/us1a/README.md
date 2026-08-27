# us1
## Process to generate source catalog directories for dataset

```bash
uv run python create_file_list.py
uv run python analyze_file_breakdown.py
uv run python create_split_sources.py --lon 6 --lat 90
```

The above process will generate a set of sibling source directories organized by longitude bands with de-duplication. The source directories will be named using a base prefix (e.g., 'us1a', 'us1b', etc.)
