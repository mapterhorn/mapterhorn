from glob import glob

import mercantile
from ulid import ULID

import local_config
import utils

def get_mercator_resolutions(minzoom, maxzoom):
    resolutions = []
    for z in range(minzoom, maxzoom + 1):
        tile = mercantile.Tile(x=0, y=0, z=z)
        bounds = mercantile.xy_bounds(tile)
        resolutions.append((bounds.right - bounds.left) / 512)
    return resolutions

def bounds_intersect(a, b):
    left_a, bottom_a, right_a, top_a = a
    left_b, bottom_b, right_b, top_b = b
    dont_intersect = False
    dont_intersect |= right_a <= left_b
    dont_intersect |= right_b <= left_a
    dont_intersect |= top_a <= bottom_b
    dont_intersect |= top_b <= bottom_a
    return not dont_intersect

def get_intersecting_tiles_dfs(bounds, tile, zoom):
    tile_bounds = mercantile.xy_bounds(tile)
    if not bounds_intersect(bounds, tile_bounds):
        return []
    if tile.z == zoom:
        return [tile]
    result = []
    for child in mercantile.children(tile, zoom=tile.z + 1):
        result += get_intersecting_tiles_dfs(bounds, child, zoom)
    return result

def get_macrotile_map():
    macrotile_map = {}
    filepaths = sorted(glob(f'source-store/*/bounds.csv'))
    mercator_resolutions = get_mercator_resolutions(0, 32)
    for filepath in filepaths:
        print(f'reading {filepath}...')
        source = filepath.split('/')[1]
        with open(filepath) as f:
            f.readline() # skip header
            line = f.readline().strip()
            while line != '':
                filename, left, bottom, right, top, width, height, crs = line.split(',')
                width, height = [int(a) for a in [width, height]]
                left, bottom, right, top = [float(a) for a in [left, bottom, right, top]]

                multiplier = 2
                buffer = multiplier * local_config.macrotile_buffer_3857
                buffered_bounds = (
                    left - buffer,
                    bottom - buffer,
                    right + buffer,
                    top + buffer
                )

                tiles = get_intersecting_tiles_dfs(buffered_bounds, mercantile.Tile(x=0, y=0, z=0), local_config.macrotile_z)
                
                maxzoom = get_smallest_overzoom(left, bottom, right, top, width, height, mercator_resolutions)

                # Use at least a maxzoom of 12 (macrotile_z).
                # Note that glo30 does not everywhere give a maxzoom of 12. Examples:
                # S79 native maxzoom 9
                # N66 native maxzoom 10
                # N50 native maxzoom 11
                # N49 native maxzoom 12 (group has only ~30 percent of total macrotiles)
                # Use gdal warp with cubicspline when maxzoom is 12
                maxzoom = max(maxzoom, local_config.macrotile_z)

                for tile in tiles:
                    if (tile.x, tile.y) not in macrotile_map:
                        macrotile_map[(tile.x, tile.y)] = {'sources': {}}
                    if source not in macrotile_map[(tile.x, tile.y)]['sources']:
                        macrotile_map[(tile.x, tile.y)]['sources'][source] = []
                    macrotile_map[(tile.x, tile.y)]['sources'][source].append({
                        'filename': filename,
                        'crs': crs,
                        'maxzoom': maxzoom,
                    })
                line = f.readline().strip()

    return macrotile_map

def get_smallest_overzoom(left, bottom, right, top, width, height, mercator_resolutions):
    horizontal_resolution = (right - left) / width
    vertical_resolution = (top - bottom) / height
    
    for z in range(len(mercator_resolutions)):
        if mercator_resolutions[z] < horizontal_resolution and mercator_resolutions[z] < vertical_resolution:
            return z
    return len(mercator_resolutions[-1])

def add_group_ids(macrotile_map):
    for tile_tuple in macrotile_map:
        group_id_parts = set({})
        for source in macrotile_map[tile_tuple]['sources']:
            for source_item in macrotile_map[tile_tuple]['sources'][source]:
                group_id_parts.add((source, source_item['maxzoom'], source_item['crs']))
        group_id = tuple(sorted(list(group_id_parts)))
        macrotile_map[tile_tuple]['group_id'] = group_id

def get_aggregation_tiles_dfs(candidate, macrotile_map):
    if candidate.z == local_config.macrotile_z:
        return [candidate]
    macrotiles = list(mercantile.children(candidate, zoom=local_config.macrotile_z))
    group_ids = set({})
    for macrotile in macrotiles:
        tile_tuple = (macrotile.x, macrotile.y)
        if tile_tuple in macrotile_map:
            group_ids.add(macrotile_map[tile_tuple]['group_id'])
    if len(group_ids) == 0:
        return []
    if len(group_ids) == 1:
        group_id = list(group_ids)[0]
        maxzoom = 0
        for part in group_id:
            maxzoom = max(maxzoom, part[1])
        if candidate.z >= maxzoom - local_config.num_overviews:
            return [candidate]
    result = []
    for child in mercantile.children(candidate, zoom=candidate.z + 1):
        result += get_aggregation_tiles_dfs(child, macrotile_map)
    return result

def get_aggregation_tiles(macrotile_map):
    candidates = set({})
    for tile_tuple in macrotile_map.keys():
        candidates.add(mercantile.parent(mercantile.Tile(x=tile_tuple[0], y=tile_tuple[1], z=local_config.macrotile_z), zoom=local_config.macrotile_z - local_config.num_overviews))
    aggregation_tiles = []
    for candidate in candidates:
        aggregation_tiles += get_aggregation_tiles_dfs(candidate, macrotile_map)
    return aggregation_tiles

def write_aggregation_items(macrotile_map, aggregation_tiles, aggregation_id):
    folder = f'aggregation-store/{aggregation_id}'
    utils.create_folder(folder)
    for aggregation_tile in aggregation_tiles:
        macrotiles = list(mercantile.children(aggregation_tile, zoom=local_config.macrotile_z))
        lines = ['source,filename,crs,maxzoom\n']
        line_tuples = set({})
        child_z = 0
        for macrotile in macrotiles:
            tile_tuple = (macrotile.x, macrotile.y)
            if tile_tuple not in macrotile_map:
                continue
            for source in macrotile_map[tile_tuple]['sources']:
                for source_item in macrotile_map[tile_tuple]['sources'][source]:
                    line_tuples.add((
                        source, 
                        source_item['filename'], 
                        source_item['crs'], 
                        str(source_item['maxzoom']
                    )))
                    child_z = max(child_z, source_item['maxzoom'])
        line_tuples = sorted(list(line_tuples))
        for line_tuple in line_tuples:
            lines.append(f'{",".join(line_tuple)}\n')
        with open(f'{folder}/{aggregation_tile.z}-{aggregation_tile.x}-{aggregation_tile.y}-{child_z}-aggregation.csv', 'w') as f:
            f.writelines(lines)

def main():

    remote_source_store = f'{local_config.remote_source_store_path}/'
    local_source_store = f'source-store/'

    utils.create_folder(local_source_store)
    utils.rsync(src=remote_source_store, dst=local_source_store, skip_data_files=True)

    print('get_macrotile_map...')
    macrotile_map = get_macrotile_map()

    print('add group ids...')
    add_group_ids(macrotile_map)

    print('get aggregation tiles...')
    aggregation_tiles = get_aggregation_tiles(macrotile_map)

    remote_aggregation_store = f'{local_config.remote_aggregation_store_path}/'
    local_aggregation_store = 'aggregation-store/'
    utils.create_folder(local_aggregation_store)
    utils.rsync(src=remote_aggregation_store, dst=local_aggregation_store, skip_data_files=True)

    aggregation_id = str(ULID())
    print('write aggregation items...')
    write_aggregation_items(macrotile_map, aggregation_tiles, aggregation_id)

    utils.rsync(src=local_aggregation_store, dst=remote_aggregation_store, skip_data_files=True)

if __name__ == '__main__':
    main()
