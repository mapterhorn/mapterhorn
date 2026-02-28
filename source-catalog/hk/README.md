# Hong Kong

uses the 0.5 LiDAR DTM collected in 2020 and available under the government portal's set of CC BY-ish [terms and conditions](https://portal.csdi.gov.hk/csdi-webpage/doc/TNC). The file list was generated from the [tile index](https://portal.csdi.gov.hk/geoportal/?lang=en&datasetId=cedd_rcd_1629267205233_87895) using the following script:

```python
import requests

r = requests.get('https://portal.csdi.gov.hk/server/services/common/cedd_rcd_1629267205233_87895/MapServer/WFSServer?service=wfs&request=GetFeature&typenames=DTM_2020&outputFormat=geojson&count=3260')

with open('file_list.txt', 'w') as flist:
    flist.writelines(list(tiles['properties']['URL'] + '\n' for tiles in r.json()['features']))

```
