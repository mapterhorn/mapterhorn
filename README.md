<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://mapterhorn.github.io/.github/brand/screen/mapterhorn-logo-darkmode.png">
  <source media="(prefers-color-scheme: light)" srcset="https://mapterhorn.github.io/.github/brand/screen/mapterhorn-logo.png">
  <img alt="Logo" src="https://mapterhorn.github.io/.github/brand/screen/mapterhorn-logo.png">
</picture>

Public terrain tiles for interactive web map visualizations

## Viewer

[https://mapterhorn.com/viewer](https://mapterhorn.com/viewer)

## Examples

[https://mapterhorn.com/examples](https://mapterhorn.com/examples)

## Migrate from AWS Elevation Tiles (Tilezen Joerd)

```diff
"hillshadeSource": {
    "type": "raster-dem",
-   "tiles": ["https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png"],
+   "tiles": ["https://tiles.mapterhorn.com/{z}/{x}/{y}.webp"],
    "encoding": "terrarium",
-   "tileSize": 256,
+   "tileSize": 512,
}

```

## Contributing

[CONTRIBUTING.md](./CONTRIBUTING.md)

## License

Code: BSD-3, see [LICENSE](https://github.com/mapterhorn/mapterhorn/blob/main/LICENSE).

Terrain data: various open-data sources, for a full list see [https://mapterhorn.com/attribution](https://mapterhorn.com/attribution).
