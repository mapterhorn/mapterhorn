from glob import glob
from typing import Optional

from mercantile import Tile


def find_existing_geo_tiffs() -> list[str]:
    geo_tiff_paths = glob("geotiff-store/**/*.tif", recursive=True)
    tiles = []
    for path in geo_tiff_paths:
        if get_zxy_child_z_if_well_formed(path) is None:
            print(f"Ignoring geotiff: {path}")
        else:
            tiles.append(path)
    return tiles


def get_tile(path: str) -> Tile:
    z, x, y, _ = get_zxy_child_z(path)
    return Tile(x=int(x), y=int(y), z=int(z))


def get_child_z(path: str) -> int:
    return get_zxy_child_z(path)[3]


def get_zxy_child_z_if_well_formed(path: str) -> Optional[tuple[int, int, int, int]]:
    filename = path.split("/")[-1].removesuffix(".tif")
    parts = filename.split("-")
    if len(parts) != 4 or not all(part.isdigit() for part in parts):
        return None

    z, x, y, child_z = map(int, parts)
    return z, x, y, child_z


def get_zxy_child_z(path: str) -> tuple[int, int, int, int]:
    parts = get_zxy_child_z_if_well_formed(path)
    if parts is None:
        raise ValueError(f"Could not parse z, x, y, child_z from path: {path}")
    return parts
