CREATE USER hive_metastore WITH PASSWORD 'hive_metastore_password';
CREATE DATABASE hive_metastore OWNER hive_metastore;

CREATE USER airflow WITH PASSWORD 'airflow_password';
CREATE DATABASE airflow OWNER airflow;

CREATE USER datahub WITH PASSWORD 'datahub_password';
CREATE DATABASE datahub OWNER datahub;
