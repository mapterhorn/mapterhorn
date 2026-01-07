"""Verify remote downloads match expected size and MD5 checksums post-upload.

This module validates CDN integrity after upload.py completes:

**Verification workflow**:
1. Download download_urls.json from CDN (current live manifest)
2. For each item in manifest:
   a. HEAD request: Check Content-Length matches expected size
   b. Full download + md5sum: Verify checksum matches
   c. Print result: "good" (pass) or "bad" (fail)
3. Run verifications in parallel (32 workers) for speed

**Why verify**:
- CDN corruption: Uploads can fail silently or partially
- Network errors: Transient issues during upload
- Storage issues: Disk errors, quota problems
- Cache invalidation: Stale content served after update

**Validation methods**:
1. **Size check (fast)**:
   - HTTP HEAD request gets Content-Length header
   - Instant verification without downloading
   - Catches truncated uploads, wrong files

2. **MD5 check (thorough)**:
   - wget streams file to md5sum command
   - Verifies byte-for-byte correctness
   - Catches corruption, incomplete uploads

**Multi-CDN support**:
Commented-out base_url options enable checking:
- Cloudflare R2 (primary CDN)
- Hetzner Object Storage (mirror)
- Source Cooperative (open data mirror)

**Parallel execution**:
- multiprocessing.Pool with 32 workers
- Typical runtime: 2-5 minutes for 100 bundles
- Sequential would take 30-60 minutes (network latency)

**Output**: Console logs showing pass/fail for each bundle URL.
"""

import json
import requests
from multiprocessing import Pool

import utils

SILENT = True
PROCESSES = 32


def has_expected_size(url, expected_size):
    """Check Content-Length of a URL against an expected size."""
    r = requests.head(url)
    actual_size = int(r.headers.get("Content-Length", -1))
    return actual_size == expected_size


def has_expected_md5sum(url, expected_md5sum):
    """Stream a URL and compare md5sum to expected checksum.

    Verification process:
    1. wget {url} -O -: Download to stdout (no disk writes)
    2. Pipe to md5sum: Compute hash of streamed bytes
    3. Parse md5sum output: "abc123...  -\n" → extract checksum
    4. Compare to expected_md5sum (hex string)

    Streaming avoids disk I/O and temp file cleanup, enabling faster
    verification when checking many large files (50GB+ bundles).

    The md5sum command outputs format:
    ```
    abc123def456...  filename
    ```
    We split on spaces and take parts[0] to extract the hash.

    Args:
        url: HTTPS URL to download and verify
        expected_md5sum: 32-character hex MD5 checksum

    Returns:
        bool: True if checksums match, False otherwise
    """
    command = f"wget {url} -O - | md5sum"
    out, _ = utils.run_command(command, silent=SILENT)
    parts = out.split(" ")
    assert len(parts) > 0
    return parts[0] == expected_md5sum


def has_expected_size_and_md5sum(url, expected_size, expected_md5sum):
    """Return True if both size and md5 checks pass."""
    if not has_expected_size(url, expected_size):
        return False
    if not has_expected_md5sum(url, expected_md5sum):
        return False
    return True


def print_check(url, expected_size, expected_md5sum):
    """Print validation result for a single URL."""
    print("working on", url)
    if has_expected_size_and_md5sum(url, expected_size, expected_md5sum):
        print(url, "good")
    else:
        print(url, "bad")


def main():
    """Validate download_urls.json items against current remote storage."""
    r = requests.get("https://download.mapterhorn.com/download_urls.json")
    data = json.loads(r.text)

    base_url = "https://download.mapterhorn.com/"  # Cloudflare
    # base_url = 'https://nbg1.your-objectstorage.com/mapterhorn/' # Hetzner
    # base_url = 'https://data.source.coop/mapterhorn/mapterhorn/' # Source Coop

    argument_tuples = []
    for item in data["items"]:
        url = f"{base_url}{item['name']}"
        argument_tuples.append((url, item["size"], item["md5sum"]))

    with Pool(PROCESSES) as pool:
        pool.starmap(print_check, argument_tuples, chunksize=1)


if __name__ == "__main__":
    main()
