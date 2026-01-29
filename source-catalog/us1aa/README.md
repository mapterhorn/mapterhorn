# `us1*`

This source was created from the USGS 1 m DEM data.
The files were split by CRS into groups giving the first letter appendix. a is Hawaii West, b is Hawaii East, c is Pacific North-West, etc...
Within these CRS groups the files were further split in the North South direction giving the second letter appendix. For this latitude splitting the code used was:

```python
if y < 400:
    latitude_group = 'a'
elif 400 <= y <= 475:
    latitude_group = 'b'
else:
    latitude_group = 'c'
```