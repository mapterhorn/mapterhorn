#!/bin/bash
# https://basisdata.nl/hwh-ahn/AUX/bladwijzer/index.html?
# https://fsn1.your-objectstorage.com/hwh-ahn/AHN5_KM/02a_DTM_50cm/AHN5_M_124000_481000.TIF
AHN5_ITEMS="https://api.ellipsis-drive.com/v3/ogc/features/65945b69-81df-4270-97f0-f029033154c1/collections/01170035-93d3-4a38-b04c-8e7be7a7ca78/items"
AHN6_ITEMS="https://api.ellipsis-drive.com/v3/ogc/features/0820faae-5240-499b-8486-cf406433cf71/collections/6aec07f5-f7eb-4f51-b6f7-aee45e5767bd/items"

# Get via OGC API Features AHN5
curl ${AHN5_ITEMS} | jq > file_list_ahn5.json
grep 'Maaiveldmodel (DTM) ½m'   file_list_ahn5.json | sort -u | cut -d '"' -f4 > file-list-AHN5-DTM-50cm.txt

curl ${AHN6_ITEMS} | jq > file_list_ahn6.json
grep 'Maaiveldmodel (DTM) ½m'   file_list_ahn6.json | sort -u | cut -d '"' -f4 > file-list-AHN6-DTM-50cm.txt
