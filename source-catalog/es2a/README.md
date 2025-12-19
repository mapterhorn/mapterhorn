# es2a

First run `python3 list_files.py > files.txt` to get a list of valid file numbers.

Then run `python3 download.py` to download the tifs.

Then split the files by CRS such that:

EPSG:4326 -> es2a
EPSG:25830 -> es2b
EPSG:25829 -> es2c

Remove the remaining files