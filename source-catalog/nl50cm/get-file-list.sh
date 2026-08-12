#!/bin/bash
# See https://www.ahn.nl/dataroom
# In 2026 AHN5 and AHN6 each are incomplete for NL, but together cover whole NL.
# Revisit in 2027!

AHN5_ITEMS="https://api.ellipsis-drive.com/v3/ogc/features/65945b69-81df-4270-97f0-f029033154c1/collections/01170035-93d3-4a38-b04c-8e7be7a7ca78/items"
AHN6_ITEMS="https://api.ellipsis-drive.com/v3/ogc/features/0820faae-5240-499b-8486-cf406433cf71/collections/6aec07f5-f7eb-4f51-b6f7-aee45e5767bd/items"

python getfilelist.py ${AHN5_ITEMS} "Maaiveldmodel (DTM) ½m" ahn5-files.txt
python getfilelist.py ${AHN6_ITEMS} "Maaiveldmodel (DTM) ½m" ahn6-files.txt

echo "merging: AHN5-DTM-50cm: $(wc -l ahn5-files.txt) files + AHN6-DTM-50cm: $(wc -l ahn6-files.txt) files"
cat ahn5-files.txt ahn6-files.txt | sort -u | sed '/^$/d' > file_list.txt
/bin/rm ahn5-files.txt ahn6-files.txt
