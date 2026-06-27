from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import json

from gustavo_sdk.app.airflow_functions import postgres_to_bucket

logger = logging.getLogger(__name__)

conn_postgres = BaseHook.get_connection("postgres_default")
host = conn_postgres.host
user = conn_postgres.login
password = conn_postgres.password
port = conn_postgres.port or 3306
database = conn_postgres.schema

conn_bq = BaseHook.get_connection("gcp_default")
service_account_info = json.loads(
    conn_bq.extra_dejson["keyfile_dict"]
)

with DAG(
    dag_id="ingestion_postgres_ecomercegustavo_landing",
    start_date=datetime(2026, 6, 14),
    schedule=None,
    catchup=False,
) as dag:

    user_ingestion = PythonOperator(
        task_id="user_ingestion",
        python_callable=postgres_to_bucket,
        op_kwargs={
            "host": host,
            "user": user,
            "password": password,
            "port": port,
            "database": database,
            "table_name": "usuario",
            "bucket_name": "ecommercegustavo-data-lake",
            "bucket_prefix": "ecomercegustavo/usuario",
            "credentials_json": service_account_info,
        }
    )

    product_ingestion = PythonOperator(
        task_id="product_ingestion",
        python_callable=postgres_to_bucket,
        op_kwargs={
            "host": host,
            "user": user,
            "password": password,
            "port": port,
            "database": database,
            "table_name": "produto",
            "bucket_name": "ecommercegustavo-data-lake",
            "bucket_prefix": "ecomercegustavo/usuario",
            "credentials_json": service_account_info,
        }
    )