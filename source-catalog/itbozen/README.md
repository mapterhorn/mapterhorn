# itbozen

Two files have a roughly 1 pixel wide artifact. The elevation is 220 m instead of 2200 m roughly.

To fix those to files, we run the following commands:


```bash
uv run python source_set_nodata.py itbozen -99999 --force

gdal_translate \
  -a_srs EPSG:25832 \
  -projwin 659530.1959 5154778.5716 659796.0318 5151666.5190 \
  source-store/itbozen/DigitalTerrainModel-2_5m_4.tif \
  cropped.tif

gdal_calc.py \
  -A cropped.tif \
  --outfile=dem_masked.tif \
  --calc="where(A < 1000, -9999, A)" \
  --type=Float32 \
  --overwrite

gdalbuildvrt -o mosaic.vrt source-store/itbozen/DigitalTerrainModel-2_5m_4.tif dem_masked.tif
gdal_translate -a_nodata -999999 mosaic.vrt mosaic.tif

gdal_calc.py \
  -A mosaic.tif \
  --outfile=source-store/itbozen/DigitalTerrainModel-2_5m_4_fixed.tif \
  --calc="where(A < 0, -9999, A)" \
  --type=Float32 \
  --overwrite

rm source-store/itbozen/DigitalTerrainModel-2_5m_4.tif

gdal_translate \
  -a_srs EPSG:25832 \
  -projwin 659647.3167 5151794.2330 659780.1269 5151029.9101 \
  source-store/itbozen/DigitalTerrainModel-2_5m_1.tif \
  cropped.tif

gdal_calc.py \
  -A cropped.tif \
  --outfile=dem_masked.tif \
  --calc="where(A < 1000, -9999, A)" \
  --type=Float32 \
  --overwrite

gdalbuildvrt -o mosaic.vrt source-store/itbozen/DigitalTerrainModel-2_5m_1.tif dem_masked.tif
gdal_translate -a_nodata -999999 mosaic.vrt mosaic.tif

gdal_calc.py \
  -A mosaic.tif \
  --outfile=source-store/itbozen/DigitalTerrainModel-2_5m_1_fixed.tif \
  --calc="where(A < 0, -9999, A)" \
  --type=Float32 \
  --overwrite

rm source-store/itbozen/DigitalTerrainModel-2_5m_1.tif

uv run python source_set_nodata.py itbozen -9999 --force

uv run python source_to_cog.py itbozen
```