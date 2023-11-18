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

def extract_tables():
    raw_file = '/usr/local/data/0_raw/vendas-combustiveis-m3.xls'
    trust_path = '/usr/local/data/1_trusted/'

    produtc_list = ['ÓLEO DIESEL TOTAL','GLP TOTAL']
    list_mes = ['JANEIRO','FEVEREIRO','MARÇO','ABRIL','MAIO','JUNHO','JULHO','AGOSTO','SETEMBRO','OUTUBRO','NOVEMBRO','DEZEMBRO']
    list_ano = str(list(range(1995,2023))).strip('[]').split(', ')

    df = pd.read_excel(raw_file,engine='xlrd', keep_default_na=False)

    table_ref = get_idx_product(df,produtc_list)

    for key in table_ref.keys():

        print(table_ref[key])
        for idx in table_ref[key]:
            dt = get_table(df,idx,list_mes,list_ano,key)
            
            dt.to_csv(f'{trust_path}/{idx}_tables_{datetime.utcnow().strftime("%Y-%m-%d_%H.%M.%S")}.csv', index = False)
