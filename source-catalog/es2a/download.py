import requests
from multiprocessing import Pool
import subprocess
from glob import glob

OUTDIR = '../../pipelines/source-store/es2/'
SILENT = False

def run_command(command, silent=True):
    if not silent:
        print(command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    err = stderr.decode()
    if err != '' and not silent:
        print(err)
    out = stdout.decode()
    if out != '' and not silent:
        print(out)
    return out, err

def download(file_number):
    url = 'https://centrodedescargas.cnig.es/CentroDescargas/descargaDir'

    data = {
        'secDescDirLA': f'{file_number}',
        'secuencial': '',
        'codNumMD': '',
        'idsMenciones': 'Modelo Digital del Terreno 2 m 2ª cobertura',
        'texto': '',
        'tipoReport': '',
        'codAgr': 'MOMDT',
        'codSerie': 'MDT02',
        'codSerieVisor': '',
        'numPagina': '',
        'coordenadas': '',
        'codComAutonoma': '',
        'codProvincia': '',
        'codIne': '',
        'codTipoArchivo': '',
        'codIdiomaInf': '',
        'todaEspania': '',
        'todoMundo': '',
        'idProductor': '',
        'rutaNombre': '',
        'numHoja': '',
        'numHoja25': '',
        'totalArchivos': '8306',
        'urlProd': 'modelo-digital-terreno-mdt02-segunda-cobertura',
        'sec': '',
        'codSubserie': '',
        'filtroOrderBy': '',
        'lon': '',
        'lat': '',
        'avisoLimiteFiles': '',
        'nomFormato': '',
        'nomTematica': '',
        'ambitoGeografico': '',
        'keySearch': '',
        'comboComSerie': '',
        'comboProvSerie': '',
        'filtroNumHoja': '',
        'referCatastral': '',
        'coordLat': '',
        'comboTipoArchSerie': '',
        'licenciaSeleccionada': '32',
    }

    r = requests.post(
        url,
        data=data,
        allow_redirects=True,
    )

    if r.status_code == 200:
        print(f'sucessfully downloaded {file_number}')
        tmp_filepath = f'{OUTDIR}/tmp-{file_number}.tif'
        with open(tmp_filepath, 'wb') as f:
            f.write(r.content)
        
        command = f'gdal_translate {tmp_filepath} {OUTDIR}/{file_number}.tif -of COG -co BLOCKSIZE=512 -co OVERVIEWS=NONE -co SPARSE_OK=YES -co BIGTIFF=YES -co COMPRESS=LERC -co MAX_Z_ERROR=0.001'
        run_command(command, silent=SILENT)

        run_command(f'rm {tmp_filepath}', silent=SILENT)

def main():
    argument_tuples = []
    already_downloaded = set({})
    for filepath in glob(f'{OUTDIR}/*'):
        filename = filepath.split('/')[-1]
        file_number = filename.replace('.tif', '')
        already_downloaded.add(file_number)

    with open('files.txt') as f:
        for line in f.readlines():
            file_number = line.strip()
            if file_number not in already_downloaded:
                argument_tuples.append((file_number,))

    print('number of files to download:', len(argument_tuples))    
    parallel = True
    if parallel:
        with Pool(100) as pool:
            pool.starmap(download, argument_tuples, chunksize=1)
    else:
        for argument_tuple in argument_tuples:
            download(argument_tuple[0])

if __name__ == '__main__':
    main()
