import pandas as pd

from datetime import datetime

def get_idx_product(df,prod_list):
    table_idx = {}
    for prod in prod_list:
        list_idx = list(df[df['Unnamed: 1'].str.contains(prod)].index)
        table_idx[prod] = list_idx
    #print(table_idx)
    return table_idx

def get_table(df,idx,list_mes,list_ano,product):

    dt = df.iloc[idx:idx+18].copy()
    
    
    unit = dt.loc[df['Unnamed: 1'].str.contains(product),'Unnamed: 1'].iloc[0]
    unit = unit.split('(')[-1].strip(')')
    #print(unit)
    uf = dt.loc[(dt['Unnamed: 1'].str.contains('FED')) | (dt['Unnamed: 1'].str.contains('REG')),'Unnamed: 2'].iloc[0]
    uf = uf.split('(')[-1].strip(')')
    print(uf)
    columns = dt.loc[dt['Unnamed: 1'].eq('Dados'),].reset_index(drop = True)
    dt.columns = columns.iloc[0]
    dt = dt[dt.Dados.str.upper().isin(list_mes)]

    list_columns = list(dt.columns)
    list_columns = list(filter(lambda x: str(x) in list_ano, list_columns))
    columns = ['Dados']
    for column in list_columns:
        dt = dt.rename(columns = {column:f'cl_{column}'})
        columns.extend([f'cl_{column}'])
    dt = dt[columns].reset_index(drop=True)
    dt['Dados'] = dt['Dados'].str.upper()
    dt['product'] = product
    dt['unit'] = unit
    dt['uf'] = uf

    return  dt

def get_blob_file():
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    from datetime import datetime
    # Defina as informações do seu Blob Storage
    SOURCE_CONTAINER_NAME = 'desafioraizen/staging'
    SOURCE_BLOB_NAME = 'vendas-combustiveis-m3_staging.xls'


    # Configurações do seu Blob Storage
    WASB_CONNECTION_STRING = 'blob_storage'

    print("TEST LIST BLOBS")
    source_hook = WasbHook(wasb_conn_id=WASB_CONNECTION_STRING)
    source_hook.get_file(file_path = '/tmp/vendas-combustiveis.xls' ,container_name=SOURCE_CONTAINER_NAME, blob_name=SOURCE_BLOB_NAME)
    # blob_list = source_hook.get_blobs_list(container_name = SOURCE_CONTAINER_NAME)
    # print(list(blob_list))

    return '/tmp/vendas-combustiveis.xls'

def load_blob_file(list_tables):
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    from datetime import datetime
    # Defina as informações do seu Blob Storage
    
    DESTINATION_CONTAINER_NAME = 'desafioraizen/processing'
    
  
    # Configurações do seu Blob Storage
    WASB_CONNECTION_STRING = 'blob_storage'

    print("TEST LIST BLOBS")
    source_hook = WasbHook(wasb_conn_id=WASB_CONNECTION_STRING)

    for table in list_tables:
        table_name = str(table).split('/')[-1]
        source_hook.load_file(file_path = table, container_name=DESTINATION_CONTAINER_NAME, blob_name=table_name, overwrite=True)
    



    
    


    
def extract_tables():
    raw_file = get_blob_file()
    


    produtc_list = ['ÓLEO DIESEL TOTAL','GLP TOTAL']
    list_mes = ['JANEIRO','FEVEREIRO','MARÇO','ABRIL','MAIO','JUNHO','JULHO','AGOSTO','SETEMBRO','OUTUBRO','NOVEMBRO','DEZEMBRO']
    list_ano = str(list(range(1995,2023))).strip('[]').split(', ')

    df = pd.read_excel(raw_file,engine='xlrd', keep_default_na=False)

    table_ref = get_idx_product(df,produtc_list)

    list_tables = []

    for key in table_ref.keys():

        print(table_ref[key])
        for idx in table_ref[key]:
            dt = get_table(df,idx,list_mes,list_ano,key)

            name_table = f'{idx}_tables.csv' #_{datetime.utcnow().strftime("%Y-%m-%d_%H.%M.%S")}.csv'
            print(name_table)
            list_tables.append(f'/tmp/{name_table}')

            dt.to_csv(f'/tmp/{name_table}', index = False)
    
    print(list_tables)
    
    load_blob_file(list_tables)

    
