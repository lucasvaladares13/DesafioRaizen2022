
# Airflow Spark - Desafio Raizen

This project contains the following containers:

* postgres: Postgres database for Airflow metadata.

  * Image: postgres:13
  * Database Port: 5432
  * References: https://hub.docker.com/_/postgres
* airflow-webserver: Airflow webserver and Scheduler.

  * Image: docker-airflow-spark:2.3.3
  * Port: 8080
* spark: Spark Master.

  * Image: bitnami/spark:3.1.2
  * Port: 8181
  * References:
    * https://github.com/bitnami/bitnami-docker-spark
    * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated
* spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name inside the docker-compose.yml file.

  * Image: bitnami/spark:3.1.2
  * References:
    * https://github.com/bitnami/bitnami-docker-spark
    * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated
* jupyter-spark: Jupyter notebook with pyspark for interactive development.

  * Image: jupyter/pyspark-notebook:spark-3.1.2
  * Port: 8888
  * References:
    * https://hub.docker.com/layers/jupyter/pyspark-notebook/spark-3.1.2/images/sha256-37398efc9e51f868e0e1fde8e93df67bae0f9c77d3d3ce7fe3830faeb47afe4d?context=explore
    * https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook
    * https://hub.docker.com/r/jupyter/pyspark-notebook/tags/

## Architecture components

![](./doc/architecture.png "Architecture")

## Setup

### Clone project

    $ git clone https://github.com/lucasvaladares13/DesafioRaizen2022.git

### Build airflow Docker

Inside the docker/

    $ docker build --rm --force-rm -t docker-airflow-spark:2.3.3 .

### Start airflow containers

Navigate to /docker and:

    $ docker-compose -f docker-compose.yaml up -d

### Start spark containers

Navigate to /docker and:

    $ docker-compose -f docker-compose-spark.yaml up -d

### Check if you can access

Airflow: http://localhost:8080

Spark Master: http://localhost:8181

Postgres - Database airflow:

* Server: localhost:5432
* Database: airflow
* User: airflow
* Password: airflow
