# pl1

Load features from an OGC Features API at https://mapy.geoportal.gov.pl/wss/service/PZGIK/NumerycznyModelTerenuEVRF2007/WFS/Skorowidze.

`feature_type` can be one of:

gugik:SkorowidzNMT2018
gugik:SkorowidzNMT2019
gugik:SkorowidzNMT2020
gugik:SkorowidzNMT2021
gugik:SkorowidzNMT2022
gugik:SkorowidzNMT2023
gugik:SkorowidzNMT2024
gugik:SkorowidzNMT2025

...and following years.

To get the features in GLM XML format call https://mapy.geoportal.gov.pl/wss/service/PZGIK/NumerycznyModelTerenuEVRF2007/WFS/Skorowidze with these URL query parameters:

`f'?SERVICE=WFS&REQUEST=GetFeature&TYPENAMES={feature_type}&VERSION=2.0.0&OUTPUTFORMAT=application/gml+xml; version=3.2`.

Keep only features

- with a resolution of 1 m, `feature['properties']['char_przestrz'] == '1.00 m'`, and
- that are on a 1:5000 map sheet, `feature['properties']['modul_archiwizacji'] == '1:5000'`, and
- where the feature id does not start with a numeric digit, `feature['properties']['godlo'][0] not in '0123456789'`.

For each sheet id (godlo) keep only the feature from the latest year. For the year use the feature_type variable.

Sort the remaining features by download url `feature['properties']['url_do_pobrania']`.

Print the download urls of the sorted features.

