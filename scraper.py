
# -*- coding: utf-8 -*-
import os
import datetime
import pandas as pd
import shutil
import requests
from zipfile import ZipFile
import consolida_arquivos
import merge_arquivos
import scraperwiki


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
        for data in response.iter_content():
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

    today = datetime.date.today()
    ano_final = int(today.strftime('%Y'))
    ano_inicial = ano_final - 1
    mes_final = int(today.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        for mes in range(1,13):
            # evita pegar anos futuros, visto que o arquivo ainda não existe
            if ano == ano_final and mes > mes_final:
                break

            mes = str(mes).zfill(2)
            download_arquivo(mes, ano)
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

    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

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
            processa_arquivo(file_extracted)

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
        df = pd.read_csv(file_path, sep=';', skiprows=3, encoding='latin1')
    except Exception as e:
        print('Erro ao ler arquivo', file_path, e)
        return False

    # transforma o campo saldo em número
    df['SALDO'] = df['SALDO'].str.replace(',','.')
    df['SALDO'] = pd.to_numeric(df['SALDO'])

    # remove os caracteres em brancos do nome das colunas
    df.rename(columns=lambda x: x.strip(), inplace=True)

    a_renomear = {
        '#DATA_BASE':'dt_base',
        'DOCUMENTO':'documento',
        'CNPJ':'cnpj',
        'AGENCIA':'agencia',
        'NOME_INSTITUICAO':'no_instituicao',
        'COD_CONGL':'co_conglomerado',
        'NOME_CONGL':'no_conglomerado',
        'TAXONOMIA':'taxonomia',
        'CONTA':'nu_conta',
        'NOME_CONTA':'no_conta',
        'SALDO':'saldo',
        'REALIZAVEL ATE 3M':'realizavel_ate_3m',
        'REALIZAVEL APOS 3M':'realizavel_apos_3m'
    }

    # renomeia as colunas
    df = df.rename(columns=a_renomear)

    # remove unnamed columns
    lista_ignorar = [
        'dt_base',
        'documento',
        'cnpj',
        'agencia',
        'no_instituicao',
        'no_conglomerado',
        'taxonomia',
        'no_conta'
    ]

    for coluna in df.columns:
        if coluna not in lista_ignorar:
            print(coluna)
            '''
            df[coluna] = df[coluna].astype(str)
            df[coluna] = df[coluna].str.replace('.', '')
            df[coluna] = df[coluna].str.replace(',', '.')
            df[coluna] = df[coluna].str.replace('Não', '0')
            df[coluna] = df[coluna].str.replace('Sim', '1')
            df[coluna] = df[coluna].str.replace('NI', '0')
            df[coluna] = df[coluna].str.replace('NA', '0')
            df[coluna] = df[coluna].str.replace('%', '')
            df[coluna] = df[coluna].str.replace('*', '0')
            df[coluna] = df[coluna].astype(float)
            '''
            df[coluna] = df[coluna].apply(pd.to_numeric, errors='coerce')

    # salva o file_path
    print(file_path.split('downloads')[1][1:])
    df['file'] = file_path.split('downloads')[1][1:]
    
    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=['dt_base', 'documento', 'cnpj'], data=row)

    print('{} Registros importados com sucesso'.format(len(df)))

    return True


if __name__ == '__main__':
    main()

    # rename file
    print('Renomeando arquivo sqlite')
    if os.path.exists('scraperwiki.sqlite'):
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
