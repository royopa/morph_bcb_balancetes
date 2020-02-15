
# -*- coding: utf-8 -*-
import os
import datetime
import scraperwiki
import pandas as pd
import shutil
import requests
from tqdm import tqdm
from zipfile import ZipFile


def download_file(url, file_path):
    file_path_csv = file_path.replace('.ZIP', '.CSV')
    if os.path.exists(file_path) or os.path.exists(file_path_csv):
        print('Arquivo já baixado anteriormente', file_path)
        return False

    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        print('Arquivo não encontrado', url, response.status_code)
        return False

    with open(file_path, "wb") as handle:
        print('Downloading', url)
        for data in tqdm(response.iter_content()):
            handle.write(data)
    handle.close()
    return True

    
def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except Exception:
        print("Directory", dirName, "already exists")


def main():
    create_download_folder()

    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    today = datetime.date.today()
    ano_inicial = 1995
    ano_final = int(today.strftime('%Y'))
    mes_final = int(today.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        for mes in range(1,13):
            # evita pegar anos futuros, visto que o arquivo ainda não existe
            if ano == ano_final and mes > mes_final:
                break

            mes = str(mes).zfill(2)

            download_arquivo(mes, ano)
            #processa_arquivo(mes, ano)
    return True


def download_arquivo(mes, ano):
    url_base = 'https://www4.bcb.gov.br/fis/cosif/cont/balan/'
    tipos_ifs = [
        '{}prudencial/{}{}BLOPRUDENCIAL.ZIP',
        '{}conglomerados/{}{}CONGLOMERADOS.ZIP',
        '{}bancos/{}{}BANCOS.ZIP',
        '{}liquidacao/{}{}LIQUIDACAO.ZIP',
        '{}combinados/{}{}COMBINADOS.ZIP',
        '{}sociedades/{}{}SOCIEDADES.ZIP',
        '{}consorcios/{}{}CONSORCIOS.ZIP',
        '{}cooperativas/{}{}COOPERATIVAS.ZIP'
    ]

    for tipo_if in tipos_ifs:
        # monta a URL do período
        url = tipo_if.format(url_base, ano, mes)
        
        file_name = url.split('/')[-1]
        file_path = os.path.join('downloads', file_name)
        # faz o download do arquivo na pasta       
        if download_file(url, file_path):
            file_extracted = extract_file(file_path)
            print('Arquivo extraído', file_extracted)
            os.remove(file_path)

    return True

    try:
        df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1'
        )
    except Exception:
        print('Erro ao baixar arquivo', url)
        return False

    # transforma o campo CO_PRD
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-','')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_REF'] = df['DT_COMPTC']

    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=['CO_PRD', 'DT_REF'], data=row)

    print('{} Registros importados com sucesso', len(df))
    return True


def extract_file(path_file):
    with ZipFile(path_file, 'r') as zipObj:
    # Get a list of all archived file names from the zip
        list_of_file_names = zipObj.namelist()
        # Iterate over the file names
        for file_name in list_of_file_names:
            if file_name.lower().endswith('.csv'):
                print('Extracting', file_name)
                zipObj.extract(file_name, path='downloads')
                return os.path.join('downloads', file_name)


def processa_arquivo(file_path):
    try:
        df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1'
        )
    except Exception:
        print('Erro ao baixar arquivo', url)
        return False

    # transforma o campo CO_PRD
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-','')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_REF'] = df['DT_COMPTC']

    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=['CO_PRD', 'DT_REF'], data=row)

    print('{} Registros importados com sucesso', len(df))
    return True


if __name__ == '__main__':
    print('Renomeando arquivo sqlite')
    if os.path.exists('data.sqlite'):
        shutil.copy('data.sqlite', 'scraperwiki.sqlite')

    main()

    # rename file
    print('Renomeando arquivo sqlite')
    if os.path.exists('scraperwiki.sqlite'):
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
