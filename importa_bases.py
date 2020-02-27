import os
import shutil
import pandas as pd
from sqlalchemy import create_engine
import scraperwiki

from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())


def main():
    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    engine = create_engine(os.environ.get('SQLALCHEMY_DATABASE_URI'), echo=False)


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
        df = pd.read_csv(os.path.join('bases', folder_name))

        print(df.columns)
        print(df.head())

        nome_relatorio = folder_name.split('.')[0].lower()
        nome_tabela = 'balancete_'.format(nome_relatorio)

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

        for row in df.to_dict('records'):
            scraperwiki.sqlite.save(unique_keys=['dt_base', 'documento', 'cnpj'], data=row)

        print('{} Registros importados com sucesso', len(df))
        return True

        continue

        # salva os registros no banco de dados
        df.to_sql(nome_tabela, con=engine, if_exists='replace')

        # executa para ver os resultados retornados que foram importados
        df_banco = engine.execute("SELECT * FROM {}".format(nome_tabela)).fetchall()
        print(nome_relatorio)
        print(len(df_banco))
        print('Registros importados com sucesso.')


if __name__ == '__main__':
    main()