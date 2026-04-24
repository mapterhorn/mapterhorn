import os
import shutil
from glob import glob

import mercantile

import utils


def main(filepath: str) -> None:
    _, aggregation_id, filename = filepath.split("/")

    z, x, y, child_z = [
        int(a) for a in filename.removesuffix("-aggregation.csv").split("-")
    ]

    tmp_folder = f"aggregation-store/{aggregation_id}/{z}-{x}-{y}-{child_z}-tmp"

    geotiff_done_filepath = f"{tmp_folder}/geotiff-done"
    if os.path.isfile(geotiff_done_filepath):
        print(f"geotiff {filename} already done...")
        return

    merge_done = os.path.isfile(f"{tmp_folder}/merge-done")
    if not merge_done:
        raise AssertionError(f"merge not done yet for {filepath}...")

    num_tiff_files = len(glob(f"{tmp_folder}/*.tiff"))
    tiff_filepath = f"{tmp_folder}/{num_tiff_files - 1}-3857.tiff"
    cog_filepath = f"{tmp_folder}/{num_tiff_files - 1}-3857-cog.tiff"
    left, bottom, right, top = mercantile.xy_bounds(mercantile.Tile(x=x, y=y, z=z))

    command = f"gdal_translate {tiff_filepath} {cog_filepath}"
    command += f" -projwin {left} {top} {right} {bottom}" # removes buffer
    command += " -of COG"
    command += " -co BIGTIFF=IF_NEEDED -co ADD_ALPHA=YES -co OVERVIEWS=NONE"
    command += " -co BLOCKSIZE=512 -co COMPRESS=LERC -co MAX_Z_ERROR=0.001"
    out, err = utils.run_command(command)
    if err.strip() != "":
        raise IOError(f"gdal_translate failed for {tiff_filepath}:\n{out}\n{err}")

    out_folder = utils.get_geotiff_folder(x, y, z)
    utils.create_folder(out_folder)
    out_filepath = f"{out_folder}/{z}-{x}-{y}-{child_z}.tif"
    shutil.move(src=cog_filepath, dst=out_filepath)
    utils.run_command(f"touch {geotiff_done_filepath}")
