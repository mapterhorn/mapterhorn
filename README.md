<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://mapterhorn.github.io/.github/brand/screen/mapterhorn-logo-darkmode.png">
  <source media="(prefers-color-scheme: light)" srcset="https://mapterhorn.github.io/.github/brand/screen/mapterhorn-logo.png">
  <img alt="Logo" src="https://mapterhorn.github.io/.github/brand/screen/mapterhorn-logo.png">
</picture>

Public terrain tiles for interactive web map visualizations

## Viewer

[https://mapterhorn.com/viewer](https://mapterhorn.com/viewer)

## Sources

- [Copernicus glo30](https://github.com/mapterhorn/mapterhorn/tree/main/source-catalog/glo30), 30 m resolution, global
- [swisstopo swissalti3d](https://github.com/mapterhorn/mapterhorn/tree/main/source-catalog/swissalti3d), 0.5 m resolution, Switzerland

## Download

Tiled to web mercator 512 pixel tiles. Terrarium elevation encoding. Webp image format.

- [planet.pmtiles](https://download.mapterhorn.com/planet.pmtiles): 352 GiB, global coverage, zoom 0 to 12
- [6-33-22.pmtiles](https://download.mapterhorn.com/6-33-22.pmtiles): 154 GiB, high resolution coverage in Switzerland zoom 13 to 17

## Roadmap

### Released

- ✅ Create a source catalog that lists available open digital elevation models and their licenses (v0.0.1)
- ✅ Write a pipeline to aggregate digital elevation models with different resolutions (v0.0.1)
- ✅ Apply the pipepline to a global low-resolution and a local high-resolution dataset (v0.0.1)
- ✅ Distribute the aggregated digital elevation model as PMTiles (v0.0.1)
- ✅ Write documentation for contributors explaining the pipeline

### Planned and Funded

- ◻️ Write documentation for end-users with examples 
- ◻️ Create a website with download area selection and metadata
- ◻️ High-resolution coverage accross the European Union
  - ◻️ Austria, country-wide, 10 m
  - ◻️ Austria, Burgenland, 5 m
  - ◻️ Austria, Salzburg, 1 m
  - ◻️ Austria, Oberösterreich, 0.5 m
  - ◻️ Austria, Wien, 1 m
  - ◻️ Austria, Kärnten, 5 m
  - ◻️ Austria, Salzburg, 1 m
  - ◻️ Belgium, Flanders, 1 m
  - ◻️ Belgium, Wallonie, 1 m
  - ◻️ Czech Republic, country-wide, 1 m
  - ◻️ Denmark, country-wide, 0.5 m
  - ◻️ Estonia, country-wide, 1 m
  - ◻️ Finland, country-wide, 2 m
  - ◻️ France, partial, 1 m
  - ◻️ France, country-wide, 5 m
  - ◻️ Ireland, country-wide, 2 m
  - ◻️ Italy, country-wide, 10 m
  - ◻️ Italy, Sardegna, 1 m
  - ◻️ Italy, Sicily, 2 m
  - ◻️ Italy, Bozen, 2.5 m
  - ◻️ Italy, Aosta, 0.5 m
  - ◻️ Latvia, country-wide, 1 m
  - ◻️ Lithuania, country-wide, 1 m
  - ◻️ Luxembourg, country-wide, 0.5 m
  - ◻️ Netherlands, country-wide, 0.5 m
  - ◻️ Poland, country-wide, 1 m
  - ◻️ Romania, country-wide, 5 m
  - ◻️ Slovakia, country-wide, 5 m
  - ◻️ Slovenia, country-wide, 5 m
  - ◻️ Spain, country-wide, 5 m
  - ◻️ Sweden, country-wide, 1 m
  - ◻️ Germany, Baden-Württemberg, 1 m
  - ◻️ Germany, Bayern, 1 m
  - ◻️ Germany, Berlin, 1 m
  - ◻️ Germany, Brandenburg, 1 m
  - ◻️ Germany, Bremen, 1 m
  - ◻️ Germany, Hamburg, 1 m
  - ◻️ Germany, Hessen, 1 m
  - ◻️ Germany, Mecklenburg-Vorpommern, 1 m
  - ◻️ Germany, Niedersachsen, 1 m
  - ◻️ Germany, Nordrhein-Westfalen, 1 m
  - ◻️ Germany, Rheinland-Pfalz, 1 m
  - ◻️ Germany, Saarland, 1 m
  - ◻️ Germany, Sachsen, 1 m
  - ◻️ Germany, Sachsen-Anhalt, 1 m
  - ◻️ Germany, Schleswig-Holstein, 1 m
  - ◻️ Germany, Thüringen, 1 m

### Under Consideration

- ◻️ Add more sources from countries with high-resolution open terrain data (not funded yet).

## Code

[https://github.com/mapterhorn/mapterhorn](https://github.com/mapterhorn/mapterhorn)

## License

Code: BSD-3, see [LICENSE](https://github.com/mapterhorn/mapterhorn/blob/main/LICENSE).

Source data:
- glo30: [source-catalog/glo30/LICENSE.pdf](source-catalog/glo30/LICENSE.pdf)
- swissalti3d: [source-catalog/swissalti3d/LICENSE.pdf](source-catalog/swissalti3d/LICENSE.pdf)

## About

Mapterhorn is an open-data project of [Leichter als Luft GmbH](https://leichteralsluft.ch/) that was made possible thanks to support from the NGI0 Core Fund, a fund established by [NLnet](https://nlnet.nl/) with financial support from the European Commission's [Next Generation Internet](https://ngi.eu/) programme.
