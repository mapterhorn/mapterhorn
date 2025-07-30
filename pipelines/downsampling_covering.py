from glob import glob

import mercantile

import utils
import local_config

def get_extents_from_coverings(aggregation_id, zoom):
    extents = []
    filepaths = glob(f'aggregation-store/{aggregation_id}/*-*-*-{zoom}-*.csv')
    for filepath in filepaths:
        filename = filepath.split('/')[-1]
        parts = filename.replace('.csv', '').split('-')
        extent_z, extent_x, extent_y = [int(a) for a in parts[:3]]
        extents.append(mercantile.Tile(x=extent_x, y=extent_y, z=extent_z))
    return extents

def get_tile_to_extent_map(extents, zoom):
    tile_to_extent_map = {}
    for extent in extents:
        for child in mercantile.children(extent, zoom=zoom):
            tile_to_extent_map[child] = extent
    return tile_to_extent_map

def get_simplified_extents(extents, zoom):
    simplified_extents_unlimited = list(mercantile.simplify(extents))
    simplified_extents = []
    for unlimited in simplified_extents_unlimited:
        if unlimited.z == zoom:
            simplified_extents.append(mercantile.parent(unlimited, zoom=zoom - 1))
        elif unlimited.z >= zoom - local_config.num_overviews:
            simplified_extents.append(unlimited)
        else:
            simplified_extents += list(mercantile.children(unlimited, zoom=zoom - local_config.num_overviews))
    return simplified_extents

def main():
    aggregation_ids = utils.get_aggregation_ids()
    aggregation_id = aggregation_ids[-1]

    command = f'rm aggregation-store/{aggregation_id}/*-downsampling.csv'
    utils.run_command(command)

    for child_zoom in reversed(range(1, 32)):
        print(f'\nchild_zoom={child_zoom}')
        print('get extents...')
        extents = get_extents_from_coverings(aggregation_id, child_zoom)

        if len(extents) == 0:
            continue

        print('get tile to extent map...')
        tile_to_extent_map = get_tile_to_extent_map(extents, child_zoom)

        print('get simplified extents...')
        simplified_extents = get_simplified_extents(extents, child_zoom)

        print('iterate over simplified extents...')
        for j, simplified_extent in enumerate(simplified_extents):
            if j % 100 == 0:
                print(f'{j} / {len(simplified_extents)}')
            involved_extents = set({})
            children = list(mercantile.children(simplified_extent, zoom=child_zoom))
            children_with_neighbors = set({})
            for child in children:
                children_with_neighbors.update(mercantile.neighbors(child))
            for child in children_with_neighbors:
                if child in tile_to_extent_map:
                    involved_extents.add(tile_to_extent_map[child])
            lines = ['filename\n']
            for involved_extent in involved_extents:
                lines.append(f'{involved_extent.z}-{involved_extent.x}-{involved_extent.y}-{child_zoom}.pmtiles\n')
            
            out_filepath = f'aggregation-store/{aggregation_id}/{simplified_extent.z}-{simplified_extent.x}-{simplified_extent.y}-{child_zoom - 1}-downsampling.csv'
            with open(out_filepath, 'w') as f:
                f.writelines(lines)

if __name__ == '__main__':
    main()
