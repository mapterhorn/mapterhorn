# dkgreenland

Has like `dk` a good ftp download server.

Install `lftp` on Ubuntu, then run:

```
lftp -u USERNAME,PASSWORD ftps://ftp.dataforsyningen.dk/DATABOKS_GROENLAND/GHM
> mirror
```

You get a USERNAME and PASSWORD from the dataforsyningen website https://dataforsyningen.dk/data/4780.