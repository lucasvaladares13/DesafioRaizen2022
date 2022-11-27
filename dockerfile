FROM apache/airflow:2.3.3

USER airflow
RUN airflow db upgrade

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

