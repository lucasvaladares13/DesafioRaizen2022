from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from scripts.download_file import download_file
from scripts.extract_tables import extract_tables
 
from datetime import datetime

###############################################
# Parameters
###############################################
spark_master = "spark/spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"

trust_path = "/usr/local/data/1_trusted"
refine_path = "/usr/local/data/2_refined"

postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

default_args = {
    'owner': 'Lucas Valadares',
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["lucasvao96@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False
}


 
with DAG('desafio_raizen', default_args=default_args, schedule_interval=None ) as dag:
 
    download_file = PythonOperator(
        task_id='download_file',
        python_callable = download_file
        
    )

    extract_tables = PythonOperator(
        task_id='extract_tables',
        python_callable = extract_tables
        
    )

    dbr_notebook_processing_tables = {"notebook_path": "/Users/lucas.acipreste@aluno.ufop.edu.br/Compile_Tables"}
    databricks_job_processing_tables = DatabricksSubmitRunOperator(
        task_id="Databricks_job_processing_tables",
        databricks_conn_id="azure_databricks", 
        existing_cluster_id="1121-035510-bj51vk5w",
        notebook_task= dbr_notebook_processing_tables,
        
        )
    
    dbr_notebook_upload_to_SQL = {"notebook_path": "/Users/lucas.acipreste@aluno.ufop.edu.br/Upload_To_Azure_SQL"}
    upload_to_SQL = DatabricksSubmitRunOperator(
        task_id="Databricks_job_load_data_to_SQL",
        databricks_conn_id="azure_databricks", 
        existing_cluster_id="1121-035510-bj51vk5w",
        notebook_task= dbr_notebook_upload_to_SQL,
        
        )

 
download_file>>extract_tables>>databricks_job_processing_tables>>upload_to_SQL

