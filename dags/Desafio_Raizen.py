from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
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

    spark_job_processing_tables = SparkSubmitOperator(
        task_id="spark_job_processing_tables",
        application="/usr/local/spark/app/processing_tables.py", # Spark application path created in airflow and spark cluster
        name="processing_tables",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args=[trust_path,refine_path],
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        )
 
download_file>>extract_tables>>spark_job_processing_tables

