"""
ScienceBase DEM Crawler for USGS 1m 3DEP Downloadable Data

This script crawls the USGS 1 Meter Digital Elevation Models (DEMs) collection
from ScienceBase and extracts all available GeoTIFF download links. It provides
both simple (URI-only) and detailed views of the resources, with optional 
summary statistics.

The script queries the ScienceBase Catalog API:
https://www.sciencebase.gov/catalog/item/543e6b86e4b0fd76af69cf4c

Usage:
    python wms-crawl.py                     # List all 1m TIFF download URIs
    python wms-crawl.py --detail            # Show detailed information
    python wms-crawl.py --detail --summary  # Full detail with summary stats
    python wms-crawl.py --summary           # Show summary statistics only

The script lists all downloadable 1-meter TIFF files (child items) from the 
USGS 3DEP collection, which contains over 110,000 elevation model tiles.
"""

import requests
import time
import argparse
from typing import List, Dict, Optional
import sys

# Parent item ID for the USGS 1m DEM collection on ScienceBase
PARENT_ITEM_ID = "543e6b86e4b0fd76af69cf4c"
BASE_API_URL = "https://www.sciencebase.gov/catalog/items"

def fetch_json(url: str, delay: float = 0.2) -> dict:
    """
    Fetch JSON data from a URL with a configurable delay.
    
    Args:
        url: The URL to fetch from
        delay: Delay in seconds before making the request (default: 0.2)
    
    Returns:
        dict: Parsed JSON response
    
    Raises:
        requests.HTTPError: If the HTTP request fails
    """
    time.sleep(delay)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        raise

def extract_tiff_downloads(item: dict) -> List[Dict]:
    """
    Extract GeoTIFF download information from a ScienceBase item.
    
    Looks for download links in the webLinks array that point to TIFF files.
    Extracts the download URI, filename, file size, and metadata.
    
    Args:
        item: ScienceBase item dictionary (must be full item, not list summary)
    
    Returns:
        List[Dict]: List of download details with keys:
            - uri: Direct download URL
            - filename: Extracted filename
            - title: Item title
            - size: File size in bytes (if available)
            - item_id: ScienceBase item ID
    """
    downloads = []
    
    if 'webLinks' not in item:
        return downloads
    
    for link in item.get('webLinks', []):
        # Look for download links that point to TIFF files
        link_type = link.get('type', '').lower()
        uri = link.get('uri', '')
        title = link.get('title', '')
        
        # Check if this is a download link for a TIFF file
        if link_type == 'download' and (uri.lower().endswith('.tif') or uri.lower().endswith('.tiff')):
            filename = uri.split('/')[-1] if uri else 'unknown'
            size = link.get('length')  # ScienceBase provides 'length' for file size
            
            downloads.append({
                'uri': uri,
                'filename': filename,
                'title': item.get('title', ''),
                'size': size,
                'item_id': item.get('id', ''),
                'type': title  # The link title, e.g., "TIFF"
            })
    
    return downloads

def fetch_full_item(item_id: str, delay: float = 0.2) -> Optional[dict]:
    """
    Fetch the full details of a single ScienceBase item.
    
    The list endpoint doesn't include webLinks, so we need to fetch each
    item individually to get download information.
    
    Args:
        item_id: ScienceBase item ID
        delay: Delay in seconds before making the request
    
    Returns:
        dict: Full item details, or None if fetch fails
    """
    url = f"https://www.sciencebase.gov/catalog/item/{item_id}?format=json"
    try:
        time.sleep(delay)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        # Silently skip items that can't be fetched
        return None

def crawl_collection(parent_id: str, limit: int = 100, delay: float = 0.2, 
                    max_items: Optional[int] = None) -> List[Dict]:
    """
    Crawl a ScienceBase collection and extract all TIFF download links.
    
    Iterates through paginated results from the ScienceBase API. For each item
    in the collection, fetches the full item details to extract GeoTIFF download
    links. This is necessary because the list endpoint doesn't include webLinks.
    
    Args:
        parent_id: ScienceBase parent item ID to crawl
        limit: Number of items per API request (default: 100, max: 1000)
        delay: Delay in seconds between requests (default: 0.2)
        max_items: Maximum number of items to process (None for all)
    
    Returns:
        List[Dict]: List of all TIFF download details
    """
    downloads = []
    offset = 0
    total_items = 0
    items_processed = 0
    
    print(f"Crawling ScienceBase collection (parent ID: {parent_id})...\n", file=sys.stderr)
    
    while True:
        # Build the API URL with pagination
        url = f"{BASE_API_URL}?parentId={parent_id}&limit={limit}&offset={offset}&format=json"
        
        try:
            response = fetch_json(url, delay)
            
            items = response.get('items', [])
            if not items:
                break
            
            # Get total count from first request
            if offset == 0:
                total_items = response.get('total', 0)
                print(f"Found {total_items} total items to process", file=sys.stderr)
            
            # Fetch full details for each item and extract downloads
            for item_summary in items:
                item_id = item_summary.get('id')
                if not item_id:
                    continue
                
                # Fetch full item details to get webLinks
                full_item = fetch_full_item(item_id, delay)
                if full_item:
                    tiff_downloads = extract_tiff_downloads(full_item)
                    if tiff_downloads:
                        downloads.extend(tiff_downloads)
                
                items_processed += 1
                if max_items and items_processed >= max_items:
                    print(f"Reached maximum items limit ({max_items})", file=sys.stderr)
                    return downloads
                
                # Progress indicator every 100 items
                if items_processed % 100 == 0:
                    print(f"Processed {items_processed} items, found {len(downloads)} TIFF downloads...", 
                          file=sys.stderr)
            
            # Check for next page
            next_link = response.get('nextlink', {}).get('url')
            if not next_link:
                break
            
            offset += limit
            
        except Exception as e:
            print(f"Error during crawl: {e}", file=sys.stderr)
            print(f"Last successful offset: {offset}", file=sys.stderr)
            break
    
    print(f"Crawl complete. Processed {items_processed} items.", file=sys.stderr)
    return downloads

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
    Main entry point for the ScienceBase DEM crawler.
    
    Parses command-line arguments, crawls the USGS 1m DEM collection,
    and displays download links in either simple or detailed format with
    optional summary statistics.
    """
    parser = argparse.ArgumentParser(
        description='Crawl USGS 1m DEM collection and list GeoTIFF download URIs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python wms-crawl.py                          # List all TIFF URIs
    python wms-crawl.py --detail                 # Show detailed information
    python wms-crawl.py --detail --summary       # Full detail with summary
    python wms-crawl.py --summary                # Summary only
    python wms-crawl.py --max-items 1000         # Limit to 1000 items
        """)
    parser.add_argument('--detail', action='store_true', 
                       help='Show detailed information for each TIFF')
    parser.add_argument('--summary', action='store_true', 
                       help='Show summary statistics')
    parser.add_argument('--max-items', type=int, default=None,
                       help='Maximum number of items to process (default: all)')
    parser.add_argument('--delay', type=float, default=0.2,
                       help='Delay in seconds between API requests (default: 0.2)')
    
    args = parser.parse_args()
    
    # Crawl the collection
    downloads = crawl_collection(
        PARENT_ITEM_ID, 
        delay=args.delay,
        max_items=args.max_items
    )
    
    if not downloads:
        print("No GeoTIFF downloads found.")
        return
    
    print(f"\nFound {len(downloads)} downloadable GeoTIFF files:\n")
    
    total_size = 0
    
    if args.detail:
        # Detailed view with all columns
        print(f"{'Filename':<70} {'Title':<50} {'Size':<15}")
        print(f"{'URI':<140}")
        print("-" * 225)
        
        for download in downloads:
            filename = download['filename']
            title = download['title'][:50] if download['title'] else ''
            size = download['size']
            uri = download['uri']
            
            if isinstance(size, (int, float)):
                total_size += size
            
            print(f"{filename:<70} {title:<50} {format_size(size):<15}")
            print(f"{uri:<140}")
    else:
        # Simple view with URI only
        for download in downloads:
            uri = download['uri']
            size = download['size']
            
            if isinstance(size, (int, float)):
                total_size += size
            
            print(uri)
    
    # Print summary if requested
    if args.summary:
        print("\n" + ("=" * 225 if args.detail else ""))
        print(f"SUMMARY:")
        print(f"  Total Files: {len(downloads)}")
        print(f"  Combined Size: {format_size(total_size)}")

if __name__ == '__main__':
    main()
