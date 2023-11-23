from urllib import request



def download_file():
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    from datetime import datetime
    # Defina as informações do seu Blob Storage
    SOURCE_CONTAINER_NAME = 'desafioraizen/cliente'
    DESTINATION_CONTAINER_NAME = 'desafioraizen/staging'
    SOURCE_BLOB_NAME = 'vendas-combustiveis-m3.xls'
    # DESTINATION_BLOB_NAME = f'vendas-combustiveis-m3_{datetime.today().year}_{datetime.today().month}_{datetime.today().day}_{datetime.today().hour}_{datetime.today().minute}_{datetime.today().second}.xls'
    DESTINATION_BLOB_NAME = f'vendas-combustiveis-m3_staging.xls'

    # Configurações do seu Blob Storage
    WASB_CONNECTION_STRING = 'blob_storage'


    source_hook = WasbHook(wasb_conn_id=WASB_CONNECTION_STRING)
    source_hook.get_file(file_path = '/tmp/vendas-combustiveis.xls' ,container_name=SOURCE_CONTAINER_NAME, blob_name=SOURCE_BLOB_NAME)

    source_hook.load_file(file_path = '/tmp/vendas-combustiveis.xls', container_name=DESTINATION_CONTAINER_NAME, blob_name=DESTINATION_BLOB_NAME, overwrite=True)







