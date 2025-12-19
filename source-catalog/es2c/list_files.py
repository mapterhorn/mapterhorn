import requests
import re

def process_page(page):
    url = 'https://centrodedescargas.cnig.es/CentroDescargas/archivosSerie'

    data = {
        'numPagina': f'{page}',
        'codAgr': 'MOMDT',
        'codSerie': 'MDT02',
        'coordenadas': '',
        'codComAutonoma': '',
        'codProvincia': '',
        'codIne': '',
        'codTipoArchivo': '',
        'todaEspania': '',
        'todoMundo': '',
        'idProductor': '',
        'rutaNombre': '',
        'numHoja': '',
        'numHoja25': '',
        'totalArchivos': '8306',
        'keySearch': '',
        'referCatastral': '',
        'orderBy': '',
    }

    response = requests.post(
        url,
        data=data,
    )

    return re.findall(r'(?<=data-sec=")[^"]*', response.text)

def main():
    for page in range(1, 417):
        files = process_page(page)
        for file in files:
            print(file)

if __name__ == '__main__':
    main()
