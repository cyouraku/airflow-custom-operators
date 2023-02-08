FROM apache/airflow:2.5.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
            vim \
            wget \
            unzip \
            netcat \
            curl \
            build-essential \
            libsa12-dev \
            gcc \
            libmariadb-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# MySQL
RUN pip install mysqlclient

# Oracle
RUN pip install oracledb

# Airflow Providers
RUN pip install apache-airflow-providers-common-sql
RUN pip install apache-airflow-providers-mysql
RUN pip install apache-airflow-providers-oracle