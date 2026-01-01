"""
AWS S3 DEM Crawler for USGS 1m 3DEP Elevation Data

This script crawls the AWS S3 bucket containing USGS 1 Meter Digital Elevation 
Models (DEMs) and lists all available GeoTIFF download links. It queries the 
S3 List API directly without requiring AWS credentials.

The script crawls: https://prd-tnm.s3.amazonaws.com/
Prefix: StagedProducts/Elevation/1m/Projects/

Usage:
    python http-crawl.py                        # List all TIFF download URIs
    python http-crawl.py --detail               # Show detailed information
    python http-crawl.py --detail --summary     # Full detail with summary stats
    python http-crawl.py --summary              # Show summary statistics only
    python http-crawl.py --max-items 1000       # Limit processing to 1000 files
"""

import requests
import xml.etree.ElementTree as ET
import time
import argparse
from typing import List, Dict, Optional
import sys

# S3 bucket configuration
S3_BUCKET_URL = "https://prd-tnm.s3.amazonaws.com"
S3_PREFIX = "StagedProducts/Elevation/1m/Projects/"

def fetch_s3_listing(prefix: str, marker: Optional[str] = None, 
                     max_keys: int = 1000, delay: float = 0.2) -> Dict:
    """
    Fetch a page of S3 object listings using the S3 List API.
    
    The S3 website endpoint supports REST API queries with ?prefix= and ?marker=
    parameters for pagination.
    
    Args:
        prefix: S3 prefix to list objects under
        marker: Continuation token for pagination
        max_keys: Maximum number of keys to return per request
        delay: Delay in seconds before making the request
    
    Returns:
        dict: Parsed response with 'Contents', 'IsTruncated', and 'NextMarker'
    
    Raises:
        requests.HTTPError: If the HTTP request fails
    """
    time.sleep(delay)
    
    # Build the query URL
    url = f"{S3_BUCKET_URL}/?prefix={prefix}&max-keys={max_keys}"
    if marker:
        url += f"&marker={marker}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse XML response
        root = ET.fromstring(response.content)
        
        # Define the S3 namespace
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        
        return {
            'contents': root.findall('.//s3:Contents', ns),
            'is_truncated': root.findtext('.//s3:IsTruncated', 'false', ns).lower() == 'true',
            'next_marker': root.findtext('.//s3:NextMarker', '', ns),
        }
    except Exception as e:
        print(f"Error fetching S3 listing: {e}", file=sys.stderr)
        raise

def extract_tiff_from_s3_key(key_element, ns: dict) -> Optional[Dict]:
    """
    Extract TIFF file information from an S3 Contents element.
    
    Args:
        key_element: XML element representing an S3 object
        ns: XML namespace dictionary
    
    Returns:
        dict with uri, filename, size, or None if not a TIFF
    """
    key = key_element.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}Key', '')
    size = key_element.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}Size', '0')
    
    # Filter for TIFF files only
    if not key.lower().endswith(('.tif', '.tiff')):
        return None
    
    # Extract filename
    filename = key.split('/')[-1]
    
    # Build download URL
    uri = f"{S3_BUCKET_URL}/{key}"
    
    try:
        size_bytes = int(size)
    except ValueError:
        size_bytes = 0
    
    return {
        'uri': uri,
        'filename': filename,
        'key': key,
        'size': size_bytes,
    }

def crawl_s3_bucket(prefix: str = S3_PREFIX, delay: float = 0.2, 
                   max_items: Optional[int] = None) -> List[Dict]:
    """
    Crawl the S3 bucket and extract all TIFF files.
    
    Iterates through paginated S3 List API responses, extracting TIFF file
    information from each object listing.
    
    Args:
        prefix: S3 prefix to crawl (default: StagedProducts/Elevation/1m/Projects/)
        delay: Delay in seconds between requests (default: 0.2)
        max_items: Maximum number of files to process (None for all)
    
    Returns:
        List[Dict]: List of TIFF file details with uri, filename, size
    """
    tiff_files = []
    marker = None
    items_processed = 0
    total_processed = 0
    
    print(f"Crawling S3 bucket: {S3_BUCKET_URL}", file=sys.stderr)
    print(f"Prefix: {prefix}\n", file=sys.stderr)
    
    # Define the S3 namespace for XML parsing
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    
    while True:
        try:
            listing = fetch_s3_listing(prefix, marker, delay=delay)
            contents = listing.get('contents', [])
            
            if not contents:
                break
            
            # Process each object in the listing
            for key_element in contents:
                tiff_info = extract_tiff_from_s3_key(key_element, ns)
                if tiff_info:
                    tiff_files.append(tiff_info)
                
                items_processed += 1
                total_processed += 1
                
                if max_items and total_processed >= max_items:
                    print(f"Reached maximum items limit ({max_items})", file=sys.stderr)
                    return tiff_files
                
                # Progress indicator every 100 items
                if items_processed % 100 == 0:
                    print(f"Processed {total_processed} objects, found {len(tiff_files)} TIFF files...", 
                          file=sys.stderr)
            
            # Check if there are more results
            if not listing.get('is_truncated', False):
                break
            
            # Get the marker for the next request
            marker = listing.get('next_marker')
            if not marker:
                # Fallback: use the last key if NextMarker not provided
                if contents:
                    last_key = contents[-1].findtext('{http://s3.amazonaws.com/doc/2006-03-01/}Key', '')
                    marker = last_key
                else:
                    break
            
            items_processed = 0
            
        except Exception as e:
            print(f"Error during crawl: {e}", file=sys.stderr)
            print(f"Found {len(tiff_files)} TIFF files before error", file=sys.stderr)
            break
    
    print(f"Crawl complete. Processed {total_processed} objects.", file=sys.stderr)
    return tiff_files

def format_size(size: Optional[int]) -> str:
    """
    Format file size in human-readable format.
    
    Converts raw byte counts to larger units (KB, MB, GB, TB, PB) for easier
    interpretation.
    
    Args:
        size: File size in bytes, or None
    
    Returns:
        str: Formatted size string (e.g., '246.56 GB')
    """
    if size is None or not isinstance(size, (int, float)):
        return 'Unknown'
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def main():
    """
    Main entry point for the S3 DEM crawler.
    
    Parses command-line arguments, crawls the S3 bucket,
    and displays TIFF file URIs in either simple or detailed format with
    optional summary statistics.
    """
    parser = argparse.ArgumentParser(
        description='Crawl AWS S3 bucket and list USGS 1m DEM GeoTIFF download URIs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python http-crawl.py                         # List all TIFF URIs
    python http-crawl.py --detail                # Show detailed information
    python http-crawl.py --detail --summary      # Full detail with summary
    python http-crawl.py --summary               # Summary only
    python http-crawl.py --max-items 1000        # Limit to 1000 files
        """)
    parser.add_argument('--detail', action='store_true', 
                       help='Show detailed information for each TIFF')
    parser.add_argument('--summary', action='store_true', 
                       help='Show summary statistics')
    parser.add_argument('--max-items', type=int, default=None,
                       help='Maximum number of items to process (default: all)')
    parser.add_argument('--delay', type=float, default=0.2,
                       help='Delay in seconds between API requests (default: 0.2)')
    parser.add_argument('--prefix', type=str, default=S3_PREFIX,
                       help=f'S3 prefix to crawl (default: {S3_PREFIX})')
    
    args = parser.parse_args()
    
    # Crawl the S3 bucket
    tiff_files = crawl_s3_bucket(
        prefix=args.prefix,
        delay=args.delay,
        max_items=args.max_items
    )
    
    if not tiff_files:
        print("No GeoTIFF files found.")
        return
    
    print(f"\nFound {len(tiff_files)} GeoTIFF files:\n")
    
    total_size = 0
    
    if args.detail:
        # Detailed view with all columns
        print(f"{'Filename':<70} {'S3 Key':<80} {'Size':<15}")
        print(f"{'URI':<160}")
        print("-" * 245)
        
        for tiff in tiff_files:
            filename = tiff['filename']
            key = tiff['key'][:80] if len(tiff['key']) > 80 else tiff['key']
            size = tiff['size']
            uri = tiff['uri']
            
            total_size += size
            
            print(f"{filename:<70} {key:<80} {format_size(size):<15}")
            print(f"{uri:<160}")
    else:
        # Simple view with URI only
        for tiff in tiff_files:
            uri = tiff['uri']
            total_size += tiff['size']
            print(uri)
    
    # Print summary if requested
    if args.summary:
        print("\n" + ("=" * 245 if args.detail else ""))
        print(f"SUMMARY:")
        print(f"  Total Files: {len(tiff_files)}")
        print(f"  Combined Size: {format_size(total_size)}")

if __name__ == '__main__':
    main()
