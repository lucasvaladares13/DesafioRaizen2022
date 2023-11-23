from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

def download_file():
    """
    Baixa um arquivo do Blob Storage de uma localização para outra.

    Este método utiliza o WasbHook para interagir com o Blob Storage.

    Args:
        Nenhum

    Returns:
        None
    """
    # Defina as informações do seu Blob Storage
    SOURCE_CONTAINER_NAME = 'desafioraizen/cliente'
    DESTINATION_CONTAINER_NAME = 'desafioraizen/staging'
    SOURCE_BLOB_NAME = 'vendas-combustiveis-m3.xls'
    DESTINATION_BLOB_NAME = f'vendas-combustiveis-m3_staging.xls'

    # Configurações do seu Blob Storage
    WASB_CONNECTION_STRING = 'blob_storage'

    source_hook = WasbHook(wasb_conn_id=WASB_CONNECTION_STRING)
    source_hook.get_file(file_path='/tmp/vendas-combustiveis.xls', container_name=SOURCE_CONTAINER_NAME, blob_name=SOURCE_BLOB_NAME)

    source_hook.load_file(file_path='/tmp/vendas-combustiveis.xls', container_name=DESTINATION_CONTAINER_NAME,
                         blob_name=DESTINATION_BLOB_NAME, overwrite=True)

if __name__ == "__main__":
    download_file()

