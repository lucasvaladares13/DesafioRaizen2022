import pandas as pd
from datetime import datetime

def get_idx_product(df, prod_list):
    """
    Obtém os índices das linhas no DataFrame correspondentes aos produtos na lista.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados.
        prod_list (list): Lista de produtos.

    Returns:
        dict: Dicionário onde as chaves são os produtos e os valores são listas de índices.
    """
    table_idx = {}
    for prod in prod_list:
        list_idx = list(df[df['Unnamed: 1'].str.contains(prod)].index)
        table_idx[prod] = list_idx
    return table_idx

def get_table(df, idx, list_mes, list_ano, product):
    """
    Obtém uma tabela a partir do DataFrame e dos índices fornecidos.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados.
        idx (int): Índice inicial.
        list_mes (list): Lista de meses.
        list_ano (list): Lista de anos.
        product (str): Nome do produto.

    Returns:
        pd.DataFrame: DataFrame contendo a tabela extraída.
    """
    dt = df.iloc[idx:idx+18].copy()
    
    unit = dt.loc[df['Unnamed: 1'].str.contains(product), 'Unnamed: 1'].iloc[0]
    unit = unit.split('(')[-1].strip(')')
    uf = dt.loc[(dt['Unnamed: 1'].str.contains('FED')) | (dt['Unnamed: 1'].str.contains('REG')), 'Unnamed: 2'].iloc[0]
    uf = uf.split('(')[-1].strip(')')

    columns = dt.loc[dt['Unnamed: 1'].eq('Dados'), ].reset_index(drop=True)
    dt.columns = columns.iloc[0]
    dt = dt[dt.Dados.str.upper().isin(list_mes)]

    list_columns = list(dt.columns)
    list_columns = list(filter(lambda x: str(x) in list_ano, list_columns))
    columns = ['Dados']
    for column in list_columns:
        dt = dt.rename(columns={column: f'cl_{column}'})
        columns.extend([f'cl_{column}'])
    dt = dt[columns].reset_index(drop=True)
    dt['Dados'] = dt['Dados'].str.upper()
    dt['product'] = product
    dt['unit'] = unit
    dt['uf'] = uf

    return dt

def get_blob_file():
    """
    Obtém o arquivo do Blob Storage.

    Returns:
        str: Caminho do arquivo local.
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    # Defina as informações do seu Blob Storage
    SOURCE_CONTAINER_NAME = 'desafioraizen/staging'
    SOURCE_BLOB_NAME = 'vendas-combustiveis-m3_staging.xls'
    # Configurações do seu Blob Storage
    WASB_CONNECTION_STRING = 'blob_storage'

    source_hook = WasbHook(wasb_conn_id=WASB_CONNECTION_STRING)
    source_hook.get_file(file_path='/tmp/vendas-combustiveis.xls', container_name=SOURCE_CONTAINER_NAME, blob_name=SOURCE_BLOB_NAME)

    return '/tmp/vendas-combustiveis.xls'

def load_blob_file(list_tables):
    """
    Carrega tabelas para o Blob Storage.

    Args:
        list_tables (list): Lista de caminhos para os arquivos locais.

    Returns:
        None
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    # Defina as informações do seu Blob Storage
    DESTINATION_CONTAINER_NAME = 'desafioraizen/processing'
    # Configurações do seu Blob Storage
    WASB_CONNECTION_STRING = 'blob_storage'

    source_hook = WasbHook(wasb_conn_id=WASB_CONNECTION_STRING)

    for table in list_tables:
        table_name = str(table).split('/')[-1]
        source_hook.load_file(file_path=table, container_name=DESTINATION_CONTAINER_NAME, blob_name=table_name, overwrite=True)

def extract_tables():
    """
    Extrai tabelas e as carrega no Blob Storage.

    Returns:
        None
    """
    raw_file = get_blob_file()
    
    product_list = ['ÓLEO DIESEL TOTAL', 'GLP TOTAL']
    list_mes = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO']
    list_ano = list(map(str, list(range(1995, 2023))))

    df = pd.read_excel(raw_file, engine='xlrd', keep_default_na=False)
    table_ref = get_idx_product(df, product_list)

    list_tables = []

    for key in table_ref.keys():
        for idx in table_ref[key]:
            dt = get_table(df, idx, list_mes, list_ano, key)
            name_table = f'{idx}_tables.csv' 
            list_tables.append(f'/tmp/{name_table}')
            dt.to_csv(f'/tmp/{name_table}', index=False)
    
    load_blob_file(list_tables)

if __name__ == "__main__":
    extract_tables()
