#!/usr/bin/env python3
import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
import rasterio
from rasterio.crs import CRS
import sys

LINE_LIMIT = 88


@dataclass
class Metrics:
    """Metrics for a collection of raster files."""

    file_count: int = 0
    total_size_bytes: int = 0
    dimensions: set = field(default_factory=set)
    resolutions: set = field(default_factory=set)
    datatypes: set = field(default_factory=set)
    min_x: float = float("inf")
    max_x: float = float("-inf")
    min_y: float = float("inf")
    max_y: float = float("-inf")

    def update(self, src, filepath):
        """Update metrics with data from an open rasterio source."""
        self.file_count += 1
        self.total_size_bytes += filepath.stat().st_size
        self.dimensions.add((src.width, src.height))
        self.resolutions.add((abs(src.transform.a), abs(src.transform.e)))
        self.datatypes.add(src.dtypes[0])

        bounds = src.bounds
        self.min_x = min(self.min_x, bounds.left)
        self.max_x = max(self.max_x, bounds.right)
        self.min_y = min(self.min_y, bounds.bottom)
        self.max_y = max(self.max_y, bounds.top)


def format_size(size_bytes):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PiB"


def print_wkt(wkt):
    """Format WKT string, truncating if too long."""
    if not wkt:
        return print("No CRS defined")
    return print(f"WKT: {wkt[: LINE_LIMIT - 8]}...")


def print_line_break(symbol="-"):
    print("\n" + symbol * LINE_LIMIT)


def print_metrics(crs_data):
    for i, (crs_wkt, data) in enumerate(crs_data.items(), 1):
        files = data["files"]
        metrics = data["metrics"]

        print_line_break()
        print(f"CRS #{i}")

        if crs_wkt:
            try:
                crs = CRS.from_wkt(crs_wkt)
                if crs.to_epsg():
                    print(f"EPSG Code: {crs.to_epsg()}")
                if crs.name:
                    print(f"Name: {crs.name}")
            except Exception:
                pass

        print_wkt(crs_wkt)

        print(f"\nFiles: {len(files)}")
        print(f"Sample files: {files[:3]}")

        print(f"Total size: {format_size(metrics.total_size_bytes)}")
        print(
            f"Dimensions: {', '.join(f'{w}x{h}' for w, h in sorted(metrics.dimensions))}"
        )
        print(
            f"Resolutions: {', '.join(f'{x}x{y}' for x, y in sorted(metrics.resolutions))}"
        )
        print(f"Data types: {', '.join(sorted(metrics.datatypes))}")

        if metrics.min_x != float("inf"):
            print(f"Extent (X): [{metrics.min_x:.2f}, {metrics.max_x:.2f}]")
            print(f"Extent (Y): [{metrics.min_y:.2f}, {metrics.max_y:.2f}]")

    print_line_break("=")


def analyze_files(source_dir):
    crs_data = defaultdict(lambda: {"files": [], "metrics": Metrics()})
    failed_files = []

    for tif_file in source_dir.glob("*.tif"):
        try:
            with rasterio.open(tif_file) as src:
                crs_wkt = src.crs.wkt if src.crs else ""
                crs_data[crs_wkt]["files"].append(tif_file.name)
                crs_data[crs_wkt]["metrics"].update(src, tif_file)
        except Exception as e:
            print(f"Error reading {tif_file.name}: {e}", file=sys.stderr)
            failed_files.append(tif_file.name)

    if failed_files:
        raise Exception(f"Failed to open {len(failed_files)} file(s)")

    print(f"Found {len(crs_data)} unique CRS projection(s)")

    return crs_data


def main():
    parser = argparse.ArgumentParser(
        description="Verify CRS projections for dataset files and compute metrics"
    )
    parser.add_argument("dataset", help="Dataset name")
    args = parser.parse_args()

    print(f"Processing: {args.dataset}")

    source_dir = Path(__file__).parent / "source-store" / args.dataset

    if not source_dir.exists():
        print(f"Error: Source directory not found: {source_dir}", file=sys.stderr)
        sys.exit(1)

    crs_data = analyze_files(source_dir)
    print_metrics(crs_data)

    if len(crs_data) == 0:
        print("✗ ERROR: No files could be checked successfully", file=sys.stderr)
        sys.exit(1)
    elif len(crs_data) == 1:
        print("✓ All files use the same CRS projection")
        sys.exit(0)
    else:
        print("✗ ERROR: Multiple CRS projections found", file=sys.stderr)
        print("\nCannot proceed with mixed CRS projections.", file=sys.stderr)
        print(
            "Please ensure all source files use the same coordinate reference system.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
