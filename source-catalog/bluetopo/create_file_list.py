# Generate BlueTopo GeoTIFF URLs from the public NOAA S3 bucket.
import subprocess
import sys

BUCKET = "s3://noaa-ocs-nationalbathymetry-pds/BlueTopo/"
HTTPS = "https://noaa-ocs-nationalbathymetry-pds.s3.amazonaws.com/BlueTopo/"

def list_keys():
    cmd = ["aws", "s3", "ls", "--no-sign-request", "--recursive", BUCKET]
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    urls = []
    for line in p.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        key = parts[3]
        if key.endswith(".tiff") and "BlueTopo_" in key and not key.endswith(".aux.xml"):
            # recursive listing returns BlueTopo/... paths; strip prefix if present
            if key.startswith("BlueTopo/"):
                rel = key[len("BlueTopo/"):]
            else:
                rel = key
            urls.append(HTTPS + rel)
    return sorted(set(urls))

def main():
    urls = list_keys()
    out = "file_list.txt"
    with open(out, "w") as f:
        for url in urls:
            f.write(url + "\n")
    print("wrote {} URLs to {}".format(len(urls), out))

if __name__ == "__main__":
    main()
