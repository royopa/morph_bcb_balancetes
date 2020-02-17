#!/usr/bin/env python
# coding: utf-8
import os
import shutil
import pandas as pd
from pathlib import Path


def prepare_bases_folder():
    folder_path = os.path.join('bases')

    if not os.path.exists(folder_path):
        Path(folder_path).mkdir(parents=True, exist_ok=True)

    return folder_path


prepare_bases_folder()

folder_name = 'BANCOS.CSV'

files = [
    'BANCOS.CSV',
    'CONGLOMERADOS.CSV',
    'CONSORCIOS.CSV',
    'COOPERATIVAS.CSV',
    'LIQUIDACAO.CSV',
    'SOCIEDADES.CSV',
    'COMBINADOS.CSV',
    'BLOPRUDENCIAL.CSV'
]

for folder_name in files:

    download_folder = os.path.join('downloads','bases',folder_name)

    dfs = []
    for file_name in sorted(os.listdir(download_folder)):
        print(file_name)
        file_path = os.path.join(download_folder, file_name)
        df = pd.read_csv(file_path, sep=';', skiprows=3, encoding='latin1')
        # remove os caracteres em brancos do nome das colunas
        df.rename(columns=lambda x: x.strip(), inplace=True)
        # transforma o campo saldo em número
        df['SALDO'] = df['SALDO'].str.replace(',','.')
        df['SALDO'] = pd.to_numeric(df['SALDO'])
        # junta em um unico dataframe
        dfs.append(df)

    df = pd.concat(dfs, axis=0, ignore_index=True)

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

    print(df.shape)
    print(df.columns)

    df.to_csv(os.path.join('bases', folder_name), index=None)
