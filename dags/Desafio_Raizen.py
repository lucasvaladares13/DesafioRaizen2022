from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from scripts.download_file import download_file
from scripts.extract_tables import extract_tables


default_args = {
    'owner': 'Lucas Valadares',
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["lucasvao96@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False
}

with DAG('desafio_raizen', default_args=default_args, schedule_interval=None ) as dag:
    download_file_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file
    )

    extract_tables_task = PythonOperator(
        task_id='extract_tables',
        python_callable=extract_tables
    )

    dbr_notebook_processing_tables = {"notebook_path": "/Users/lucas.acipreste@aluno.ufop.edu.br/Compile_Tables"}
    databricks_job_processing_tables = DatabricksSubmitRunOperator(
        task_id="Databricks_job_processing_tables",
        databricks_conn_id="azure_databricks", 
        existing_cluster_id="1121-035510-bj51vk5w",
        notebook_task=dbr_notebook_processing_tables
    )
    
    dbr_notebook_upload_to_SQL = {"notebook_path": "/Users/lucas.acipreste@aluno.ufop.edu.br/Upload_To_Azure_SQL"}
    upload_to_SQL = DatabricksSubmitRunOperator(
        task_id="Databricks_job_load_data_to_SQL",
        databricks_conn_id="azure_databricks", 
        existing_cluster_id="1121-035510-bj51vk5w",
        notebook_task=dbr_notebook_upload_to_SQL
    )

download_file_task >> extract_tables_task >> databricks_job_processing_tables >> upload_to_SQL
