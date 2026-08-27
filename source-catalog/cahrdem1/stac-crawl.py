"""
STAC Catalog Browser for hrdem-mosaic-1m Collection

This script browses the Canadian elevation STAC catalog (hrdem-mosaic-1m collection)
and lists all available GeoTIFF assets. It provides both simple (URI-only) and detailed
views of the resources, with optional summary statistics.

Usage:
    python stac-crawl.py [asset_type] [--detail] [--summary]

Examples:
    python stac-crawl.py                    # List all DTM assets (default)
    python stac-crawl.py dsm                # List all DSM assets
    python stac-crawl.py dtm --detail       # List DTM assets with detailed info
    python stac-crawl.py --summary          # List DTM assets with summary stats
    python stac-crawl.py dtm --detail --summary  # Full detail with summary

The script queries https://datacube.services.geo.ca/stac/api/ and retrieves
resources from the AWS S3 bucket.
"""

import requests
import time
import argparse
from typing import Set, List
from urllib.parse import urljoin

def fetch_json(url: str, delay: float = 0.5) -> dict:
    """
    Fetch JSON data from a URL with a configurable delay.
    
    Args:
        url: The URL to fetch from
        delay: Delay in seconds before making the request (default: 0.5)
    
    Returns:
        dict: Parsed JSON response
    
    Raises:
        requests.HTTPError: If the HTTP request fails
    """
    time.sleep(delay)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def extract_asset_details(item: dict, asset_filter: str = None) -> List[dict]:
    """
    Extract detailed information about GeoTIFF assets from a STAC item.
    
    Iterates through assets in a STAC item and extracts information about
    GeoTIFF files (identified by media type or file extension). Optionally
    filters to a specific asset name.
    
    Args:
        item: STAC item dictionary containing assets
        asset_filter: Optional asset name to filter by (e.g., 'dtm', 'dsm')
    
    Returns:
        List[dict]: List of asset detail dictionaries with keys:
            - asset_name: Name of the asset
            - filename: Extracted filename from URL
            - size: File size in bytes
            - modified_time: Last modified datetime
            - href: URL to the file
            - title: Human-readable title
            - description: Asset description
            - roles: Asset roles
            - media_type: MIME type
    """
    assets = []
    if 'assets' in item:
        for asset_name, asset_data in item['assets'].items():
            # Skip if filter is set and this asset doesn't match
            if asset_filter and asset_name != asset_filter:
                continue
                
            if isinstance(asset_data, dict):
                href = asset_data.get('href', '')
                media_type = asset_data.get('type', '')
                
                # Check if this is a GeoTIFF file
                if 'image/tiff' in media_type or 'geotiff' in media_type.lower() or href.lower().endswith(('.tif', '.tiff', '.geotiff')):
                    # Extract filename from URL
                    filename = href.split('/')[-1] if href else 'unknown'
                    
                    # Get file size from HEAD request
                    size = get_file_size(href)
                    
                    # Try various possible date fields to find modified time
                    modified_time = (asset_data.get('created') or 
                                   asset_data.get('updated') or 
                                   item.get('datetime') or 
                                   item.get('properties', {}).get('datetime') or
                                   'Unknown')
                    
                    assets.append({
                        'asset_name': asset_name,
                        'filename': filename,
                        'size': size,
                        'modified_time': modified_time,
                        'href': href,
                        'title': asset_data.get('title', ''),
                        'description': asset_data.get('description', ''),
                        'roles': asset_data.get('roles', []),
                        'media_type': media_type
                    })
    return assets

def get_file_size(url: str) -> int:
    """
    Get file size from a remote URL using HTTP HEAD request.
    
    Makes a HEAD request to the URL to retrieve the Content-Length header,
    which indicates the file size without downloading the entire file.
    
    Args:
        url: The URL of the file
    
    Returns:
        int or str: File size in bytes, or 'Unknown' if unable to determine
    """
    try:
        response = requests.head(url, timeout=5)
        if 'content-length' in response.headers:
            return int(response.headers['content-length'])
    except Exception as e:
        pass
    return 'Unknown'

def process_items_from_collection(collection_id: str, base_url: str, asset_filter: str = None, delay: float = 0.5) -> List[dict]:
    """
    Process all items in a STAC collection and extract asset details.
    
    Iterates through paginated results from a STAC collection, extracting
    asset information from each item. Handles pagination using 'next' links
    and includes retry logic for robustness.
    
    Args:
        collection_id: ID of the STAC collection to process
        base_url: Base URL of the STAC API
        asset_filter: Optional asset name to filter by
        delay: Delay in seconds between requests (default: 0.5)
    
    Returns:
        List[dict]: List of all asset details from the collection
    """
    assets = []
    items_url = f'{base_url}collections/{collection_id}/items'
    
    retries = 0
    while items_url:
        try:
            items_response = fetch_json(items_url, delay)
            
            # Process features from the current page
            features = items_response.get('features', [])            
            for feature in features:
                asset_details = extract_asset_details(feature, asset_filter)
                if asset_details:
                    assets.extend(asset_details)

            # Find next page link for pagination
            next_link = None
            for link in items_response.get('links', []):
                if link.get('rel') == 'next':
                    next_link = link.get('href')
                    break
            
            items_url = next_link
            
        except Exception as e:
            print(f'  Error processing items from collection {collection_id}: {e}')
            time.sleep(5)
            retries += 1
            if retries == 10:
                break
    
    return assets

def process_hrdem_collection(api_url: str, asset_filter: str = None, delay: float = 0.5) -> List[dict]:
    """
    Process the hrdem-mosaic-1m collection from the STAC API.
    
    Convenience function that wraps process_items_from_collection specifically
    for the hrdem-mosaic-1m collection and prints status information.
    
    Args:
        api_url: Base URL of the STAC API
        asset_filter: Optional asset name to filter by
        delay: Delay in seconds between requests (default: 0.5)
    
    Returns:
        List[dict]: List of asset details from the collection
    """
    filter_text = f" (filtered to {asset_filter})" if asset_filter else ""
    print(f"Fetching hrdem-mosaic-1m collection from {api_url}{filter_text}...\n")
    assets = process_items_from_collection('hrdem-mosaic-1m', api_url, asset_filter, delay)
    return assets

def explore_collections(base_url: str, delay: float = 0.5) -> None:
    """
    Explore all collections and their available assets.
    
    Fetches all collections from the STAC API and displays a matrix showing
    which asset types are available in each collection. This is useful for
    discovering what data is available without filtering.
    
    Args:
        base_url: Base URL of the STAC API
        delay: Delay in seconds between requests (default: 0.5)
    """
    try:
        print(f"Exploring collections from {base_url}...\n")
        
        # Fetch all collections
        collections_url = f'{base_url}collections'
        collections_response = fetch_json(collections_url, delay)
        collections = collections_response.get('collections', [])
        
        if not collections:
            print("No collections found.")
            return
        
        # Build a matrix of collections and their assets
        collection_assets = {}
        
        print("Fetching asset types for each collection...\n")
        
        for collection in collections:
            collection_id = collection.get('id')
            collection_assets[collection_id] = set()
            
            try:
                # Get items from this collection
                items_url = f'{base_url}collections/{collection_id}/items'
                items_response = fetch_json(items_url, delay)
                features = items_response.get('features', [])
                
                # Extract all asset names from the first few items
                for feature in features[:5]:  # Check first 5 items for efficiency
                    if 'assets' in feature:
                        for asset_name in feature['assets'].keys():
                            collection_assets[collection_id].add(asset_name)
                    if len(collection_assets[collection_id]) > 0:
                        break
                
            except Exception as e:
                print(f"  Warning: Could not fetch items for {collection_id}: {e}")
        
        # Find all unique asset types
        all_assets = set()
        for assets in collection_assets.values():
            all_assets.update(assets)
        all_assets = sorted(list(all_assets))
        
        # Display matrix
        print(f"{'Collection':<40} {' '.join(f'{a:<15}' for a in all_assets)}")
        print("-" * (40 + len(all_assets) * 16))
        
        for collection_id in sorted(collection_assets.keys()):
            assets = collection_assets[collection_id]
            row = f"{collection_id:<40}"
            for asset in all_assets:
                marker = "✓" if asset in assets else " "
                row += f" {marker:<15}"
            print(row)
        
        print(f"\nTotal Collections: {len(collections)}")
        print(f"Total Asset Types: {len(all_assets)}")
        
    except Exception as e:
        print(f"Error exploring collections: {e}")

def format_size(size):
    """
    Format file size in human-readable format.
    
    Converts raw byte counts to larger units (KB, MB, GB, TB, PB) for easier
    interpretation.
    
    Args:
        size: File size in bytes, or 'Unknown' string
    
    Returns:
        str: Formatted size string (e.g., '246.56 GB')
    """
    if size == 'Unknown' or not isinstance(size, (int, float)):
        return str(size)
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def main():
    """
    Main entry point for the STAC catalog browser.
    
    Parses command-line arguments, fetches data from the STAC API, and displays
    results in either simple (URI-only) or detailed format with optional summary.
    Can also explore all collections and their available assets.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Browse STAC catalog and list resources')
    parser.add_argument('collection_id', nargs='?', default='hrdem-mosaic-1m', help='STAC collection ID to browse (default: hrdem-mosaic-1m)')
    parser.add_argument('asset_filter', nargs='?', default='dtm', help='Asset type to filter by (default: dtm)')
    parser.add_argument('--detail', action='store_true', help='Show detailed information for each resource')
    parser.add_argument('--summary', action='store_true', help='Show summary statistics at the end')
    parser.add_argument('--explore', action='store_true', help='Explore all collections and their available assets')
    args = parser.parse_args()
    
    base_url = 'https://datacube.services.geo.ca/stac/api/'
    delay = 0.05
    
    # Handle explore mode
    if args.explore:
        explore_collections(base_url, delay)
        return
    
    # Fetch assets from the collection
    # Note: Using process_items_from_collection directly instead of process_hrdem_collection
    # to support arbitrary collection IDs
    filter_text = f" (filtered to {args.asset_filter})" if args.asset_filter else ""
    print(f"Fetching {args.collection_id} collection from {base_url}{filter_text}...\n")
    
    assets = process_items_from_collection(args.collection_id, base_url, asset_filter=args.asset_filter, delay=delay)
    
    if not assets:
        print(f"No assets found in {args.collection_id} collection.")
        return
    
    print(f"Found {len(assets)} resources in {args.collection_id} collection:\n")
    
    total_size = 0
    newest_date = None
    
    if args.detail:
        # Detailed view with all columns
        print(f"{'URI':<110} {'Asset Name':<20} {'Title':<30} {'Description':<40} {'Roles':<20} {'Size':<15} {'Modified':<25}")
        print("-" * 260)
        
        for asset in assets:
            href = asset['href']
            asset_name = asset['asset_name']
            title = asset['title'][:30] if asset['title'] else ''
            description = asset['description'][:40] if asset['description'] else ''
            roles = ', '.join(asset['roles']) if asset['roles'] else ''
            size = asset['size']
            modified_time = str(asset['modified_time'])[:25]
            
            # Accumulate total size and track newest date
            if isinstance(size, (int, float)):
                total_size += size
            
            if asset['modified_time'] != 'Unknown':
                if newest_date is None or asset['modified_time'] > newest_date:
                    newest_date = asset['modified_time']
            
            print(f"{href:<110} {asset_name:<20} {title:<30} {description:<40} {roles:<20} {format_size(size):<15} {modified_time:<25}")
    else:
        # Simple view with URI only
        for asset in assets:
            href = asset['href']
            size = asset['size']
            
            # Accumulate total size and track newest date
            if isinstance(size, (int, float)):
                total_size += size
            
            if asset['modified_time'] != 'Unknown':
                if newest_date is None or asset['modified_time'] > newest_date:
                    newest_date = asset['modified_time']
            
            print(href)
    
    # Print summary only if --summary flag is specified
    if args.summary:
        print("\n" + ("=" * 260 if args.detail else ""))
        print(f"SUMMARY:")
        print(f"  Total Files: {len(assets)}")
        print(f"  Combined Size: {format_size(total_size)}")
        print(f"  Newest Modified Date: {newest_date if newest_date else 'Unknown'}")
    
if __name__ == '__main__':
    main()
