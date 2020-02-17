#!/usr/bin/env python
# coding: utf-8
import os
import shutil
import pandas as pd
from pathlib import Path

# move arquivos para uma única pasta
def move_arquivos(lista_paths, index_name):
    for file_path in sorted(lista_paths):
        folder_path = os.path.join('downloads', 'bases', index_name)
        if not os.path.exists(folder_path):
            Path(folder_path).mkdir(parents=True, exist_ok=True)

        file_path_new = os.path.join(folder_path, file_path.split('/')[-1])
        print(file_path_new)
        shutil.move(file_path, file_path_new)
    return True


# monta os arquivos em um único arquivo
def merge_arquivos(lista_paths, file_name):
    dfs = []
    for file_path in sorted(lista_paths):
        df = pd.read_csv(file_path, sep=';', skiprows=3, encoding='latin1')
        # remove os caracteres em brancos do nome das colunas
        df.rename(columns=lambda x: x.strip(), inplace=True)
        # transforma o campo saldo em número
        df['SALDO'] = df['SALDO'].str.replace(',','.')
        df['SALDO'] = pd.to_numeric(df['SALDO'])
        # junta em um unico dataframe
        dfs.append(df)

    df = pd.concat(dfs, axis=0, ignore_index=True)
    print(df.shape)
    print(df.columns)

    df.to_csv(os.path.join('bases', file_name))
    return True


def prepare_bases_folder():
    folder_path = os.path.join('bases')

    if not os.path.exists(folder_path):
        Path(folder_path).mkdir(parents=True, exist_ok=True)

    return folder_path


download_folder = os.path.join('downloads')
prepare_bases_folder()


files = {
    'BANCOS.CSV':[],
    'CONGLOMERADOS.CSV':[],
    'CONSORCIOS.CSV':[],
    'COOPERATIVAS.CSV':[],
    'LIQUIDACAO.CSV':[],
    'SOCIEDADES.CSV':[],
    'COMBINADOS.CSV':[],
    'BLOPRUDENCIAL.CSV':[]
}


for file_name in sorted(os.listdir(download_folder)):
    file_path = os.path.join(download_folder, file_name)
        
    if not file_path.lower().endswith('.csv'):
        continue

    print(file_name)
    data_base = file_name[:7]
    ano = file_name[:4]
    mes = file_name[4:6]
    print(ano, mes, file_name, file_name[6:])

    files.get(file_name[6:]).append(file_path)


for index_name in files:
    print(index_name)
    file_path = files.get(index_name)
    #merge_arquivos(file_path, index_name)
    move_arquivos(file_path, index_name)
