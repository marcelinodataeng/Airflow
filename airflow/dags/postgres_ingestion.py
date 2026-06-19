from pathlib import Path
from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LANDING_DIR = Path("/opt/airflow/landing")
LANDING_DIR.mkdir(parents=True, exist_ok=True)

dag = DAG(
    dag_id="postgres_ingestion",
    start_date=datetime(2026, 6, 14),
    schedule=None,
    catchup=False,
    )

def extract_table(table_name: str, output_file: str) -> pd.DataFrame:
    hook = PostgresHook(
    postgres_conn_id="postgres_default"
    )

    df = hook.get_pandas_df(
    f"SELECT * FROM {table_name}"
    )

    df.to_csv(
        LANDING_DIR / output_file,
        index=False
)

    return df

def extract_user():
    return extract_table(
    table_name="usuario",
    output_file="usuarios.csv"
    )

def extract_product():
    return extract_table(
    table_name="produto",
    output_file="produtos.csv"
    )

def save_user():
    pass

def save_product():
    pass

extract_user_task = PythonOperator(
    task_id="extract_user",
    python_callable=extract_user,
    dag=dag,
    )

save_user_task = PythonOperator(
    task_id="save_user",
    python_callable=save_user,
    dag=dag,
    )

extract_product_task = PythonOperator(
    task_id="extract_product",
    python_callable=extract_product,
    dag=dag,
    )

save_product_task = PythonOperator(
    task_id="save_product",
    python_callable=save_product,
    dag=dag,
    )

extract_user_task >> save_user_task
extract_product_task >> save_product_task
