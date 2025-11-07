# se

To find the list of download urls, the STAC API had to be crawled. This API rate-limits by IP address, so only one process could be used and finding the roughly 70k urls took around 24 hours of crawing. The crawling script used looked roughly like this:

```python
import requests
import time
from typing import Set, List
from urllib.parse import urljoin

def fetch_json(url: str, delay: float = 0.5) -> dict:
    time.sleep(delay)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def extract_geotiff_urls(item: dict) -> List[str]:
    urls = []
    if 'assets' in item:
        for asset_name, asset_data in item['assets'].items():
            if isinstance(asset_data, dict):
                href = asset_data.get('href', '')
                media_type = asset_data.get('type', '')
                
                if 'image/tiff' in media_type or 'geotiff' in media_type.lower() or href.lower().endswith(('.tif', '.tiff', '.geotiff')):
                    urls.append(href)
    return urls

def process_items_from_collection(collection_id: str, base_url: str, delay: float = 0.5) -> List[str]:
    urls = []
    items_url = f'{base_url}collections/{collection_id}/items'
    
    retries = 0
    while items_url:
        try:
            items_response = fetch_json(items_url, delay)
            
            features = items_response.get('features', [])            
            for feature in features:
                geotiff_urls = extract_geotiff_urls(feature)
                if geotiff_urls:
                    urls.extend(geotiff_urls)
                    for url in geotiff_urls:
                        print(url)

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
    
    return urls

def explore_stac_api(api_url: str, delay: float = 0.5) -> Set[str]:
    all_urls = set()
    
    collections_url = urljoin(api_url, 'collections')
    
    try:
        collections_response = fetch_json(collections_url, delay)
        collections = collections_response.get('collections', [])
        collections = sorted(collections, key=lambda c: c['id'])
        for collection in collections:
            collection_id = collection.get('id')
            if collection_id < 'mhm-66_4':
                continue
            collection_urls = process_items_from_collection(collection_id, api_url, delay)
            all_urls.update(collection_urls)
            
    except Exception as e:
        print(f'Error exploring STAC API: {e}')
        exit()
    
    return all_urls

def main():
    base_url = 'https://api.lantmateriet.se/stac-hojd/v1/'
    delay = 0.05
    
    geotiff_urls = explore_stac_api(base_url, delay)
    
if __name__ == '__main__':
    main()
```