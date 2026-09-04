# INSTALL

Install all tools and dependencies to run the Mapterhorn pipelines.
Assumed is a Ubuntu or other Linux install.

## Core

```shell
sudo apt install 7zip curl wget unzip jq
```
## Pixi

Pixi is used as the Python (virtual) environment and libraries manager. 
(Earlier this was done with `uv`, but `pixi` can also manage non-Python dependencies like GDAL. Basically `uv run` becomes `pixi run`.)

```shell

# Pixi install for user in ~/.pixi/bin
curl -fsSL https://pixi.sh/install.sh | sh

cd pipelines
pixi shell
pixi install
# all deps, including binaries like GDAL, Java, are installed under `.pixi` dir
```

## Planetiler

```shell
cd pipelines
wget https://github.com/onthegomap/planetiler/releases/latest/download/planetiler.jar
```
