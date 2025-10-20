## 0.0.4

### ✨ Features and improvements

- Apply Gaussian blur along dataset boundaries (#91)
- Add sources frrgealti1*: France 1m (#87)
- Add source fi: Finland 2m (#86)
- Add source sk: Slovakia 1m (#85)
- Add source desh: Schleswig-Holstein, Germany 1m (#83)
- Add source dethueringen: Thüringen, Germany 1m (#82)
- Add source desaarland: Saarland, Germany 1m (#81)
- Add source debw: Baden-Württemberg, Germany 1m (#80)
- Add source ro: Romania partial 0.5 m (#79)
- Add source aalv: Latvia 20m (#78)
- Add source es5*: Spain 5m (#77)
- Add source si: Slovenia 0.5m (#75)
- Add source dk: Denmark 0.5m (#73)
- Add source derlp: Rheinland-Pfalz, Germany 1m (#72)
- Add source itbozen: Bozen, Italy 2.5m (#70)
- Add source itaosta: Aosta, Italy 0.5m (#69)
- Add source denrw: Nordrhein-Westfalen, Germany 1m (#68)
- Add source desachsenanhalt: Sachsen-Anhalt, Germany 1m (#66)
- Add source lu: Luxembourg country-wide 1m (#65)
- Add source dehessen: Hessen, Germany 1m (#64)
- Add source debremen: Bremen, Germany 1m
- Add source dehamburg: Hamburg, Germany 1m (#62)
- Add source desachsen: Sachsen, Germany 1m (#60)
- Add source debrandenburg: Brandenburg, Germany 1m (#57)
- Add source deberlin: Berlin, Germany 1m (#56)
- Add source demv: Mecklenburg-Vorpommern, Germany 1m (#55)
- Add sources frrgealti5*: France country-wide 5m (#54)
- Add source tinitaly: Italy country-wide 10m (#53)
- Add source ee: Estonia country-wide 1m (#52)
- Add source deniedersachsen: Niedersachsen, Germany 1m (#51)
- Add source debayern: Bayern, Germany 1m (#50)
- Add source cz: Czech Republic, 5 m (#47)
- Add Justfile (#42)
- Add source bewallonie: Wallonie, Belgium 1m (#46)
- Add source beflanders: Flanders, Belgium 1m (#45)

## 0.0.3

### ✨ Features and improvements

- Add source at10: Austria country-wide 10 m (#36)
- Add source atburgenland: Land Burgenland, Austria, 5 m (#37)
- Add source atsalzburg: Land Salzburg, Austria, 1 m (#38)
- Add source atoberoesterreich: Land Oberösterreich, Austria, 0.5 m (#41)
- Add source atkaernten: Land Kärnten, Austria, 1 m (#43)

### 🐞 Bug fixes

- Remove CRS, increase vertical resolution, fix issue with three overlapping sources (#35)

## 0.0.2

Mapterhorn v0.0.2 ships smaller tiles that are optimized for size while keeping the same visual quality, missing data in Caucasus and polar regions were added, and you can now use the pmtiles cli tool to make small area extracts.

### ✨ Features and improvements

- Write clustered pmtiles, add debug pipeline (#18)
- Adjust vertical resolution to zoom level (#19)

### 🐞 Bug fixes

- Add polar regions and missing data in Caucasus (#21)

## 0.0.1

### ✨ Features and improvements

- Starting with Copernicus glo30 (30 m resolution, global) and swissalti3d (0.5 m resolution, Switzerland)
