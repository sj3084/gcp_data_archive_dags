FROM apache/airflow:2.8.4

USER root

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

USER airflow