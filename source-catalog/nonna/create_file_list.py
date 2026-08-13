# NONNA bathymetry file list helper.
# CHS publishes NONNA grids via the CHS open data portal / AWS open data.
# This script lists known public HTTPS prefixes; update ROOTS as catalogs change.

import subprocess

# Public NONNA GeoTIFF mirrors (Open Government / open data). Prefer S3 if available.
CANDIDATE_PREFIXES = [
    "s3://chinook.canada.ca/nonna/",
]

HTTPS_FALLBACK_NOTE = """# NONNA file list
# CHS NONNA products are distributed via https://data.chs-shc.ca/
# and related open-data mirrors. Run create_file_list.py after confirming
# the current public bucket/prefix, or paste direct GeoTIFF URLs below.
"""

def try_list(prefix):
    try:
        cmd = ["aws", "s3", "ls", "--no-sign-request", "--recursive", prefix]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if p.returncode != 0:
            return []
        urls = []
        for line in p.stdout.splitlines():
            parts = line.split()
            if len(parts) < 4:
                continue
            key = parts[3]
            if key.lower().endswith((".tif", ".tiff")):
                # Convert s3 path to https if possible — leave as comment for operator
                urls.append("# s3://" + key if not key.startswith("s3://") else "# " + key)
        return urls
    except Exception:
        return []

def main():
    lines = [HTTPS_FALLBACK_NOTE]
    found = False
    for prefix in CANDIDATE_PREFIXES:
        urls = try_list(prefix)
        if urls:
            found = True
            lines.extend(urls)
    if not found:
        lines.append("# No public S3 listing found automatically. Add GeoTIFF URLs manually.\n")
    with open("file_list.txt", "w") as f:
        f.write("\n".join(lines))
        if not lines[-1].endswith("\n"):
            f.write("\n")
    print("wrote file_list.txt (found={})".format(found))

if __name__ == "__main__":
    main()
