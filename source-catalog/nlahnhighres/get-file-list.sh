#!/bin/bash
# See https://www.ahn.nl/dataroom
# In 2026 AHN5 and AHN6 each are incomplete for NL, but together cover whole NL.
# Revisit in 2027!

AHN5_ITEMS="https://api.ellipsis-drive.com/v3/ogc/features/65945b69-81df-4270-97f0-f029033154c1/collections/01170035-93d3-4a38-b04c-8e7be7a7ca78/items"
AHN6_ITEMS="https://api.ellipsis-drive.com/v3/ogc/features/0820faae-5240-499b-8486-cf406433cf71/collections/6aec07f5-f7eb-4f51-b6f7-aee45e5767bd/items"

# Get via OGC API Features AHN5
curl ${AHN5_ITEMS} | jq > file_list_ahn5.json
grep 'Maaiveldmodel (DTM) ½m'   file_list_ahn5.json | sort -u | cut -d '"' -f4 > file-list-AHN5-DTM-50cm.txt
/bin/rm file_list_ahn5.json

# Get via OGC API Features AHN6
curl ${AHN6_ITEMS} | jq > file_list_ahn6.json
grep 'Maaiveldmodel (DTM) ½m'   file_list_ahn6.json | sort -u | cut -d '"' -f4 > file-list-AHN6-DTM-50cm.txt
/bin/rm file_list_ahn6.json

echo "merging: AHN5-DTM-50cm: $(wc -l file-list-AHN5-DTM-50cm.txt) files + AHN6-DTM-50cm: $(wc -l file-list-AHN6-DTM-50cm.txt) files"
cat file-list-AHN5-DTM-50cm.txt file-list-AHN6-DTM-50cm.txt | sort -u > file_list.txt
/bin/rm file-list-AHN5-DTM-50cm.txt file-list-AHN6-DTM-50cm.txt
