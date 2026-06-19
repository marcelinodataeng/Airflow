from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id="postgres_ingestion",
    start_date=datetime(2026, 6, 14),
    schedule=None
)


def extract_user() -> pd.DataFrame:
    try:
        hook = PostgresHook(
            postgres_conn_id="postgres_default"
        )

        usuarios_df = hook.get_pandas_df(
            "SELECT * FROM usuario"
        )

        usuarios_df.to_csv(
            "landing/usuarios.csv",
            index=False
        )

        return usuarios_df

    except Exception as e:
        raise e


def save_user():
    pass


def extract_product() -> pd.DataFrame:
    try:
        hook = PostgresHook(
            postgres_conn_id="postgres_default"
        )

        produtos_df = hook.get_pandas_df(
            "SELECT * FROM produto"
        )

        produtos_df.to_csv(
            "landing/produtos.csv",
            index=False
        )

        return produtos_df

    except Exception as e:
        raise e


def save_product():
    pass


extract_user_task = PythonOperator(
    task_id="extract_user",
    python_callable=extract_user,
    dag=dag
)

save_user_task = PythonOperator(
    task_id="save_user",
    python_callable=save_user,
    dag=dag
)

extract_product_task = PythonOperator(
    task_id="extract_product",
    python_callable=extract_product,
    dag=dag
)

save_product_task = PythonOperator(
    task_id="save_product",
    python_callable=save_product,
    dag=dag
)


extract_user_task >> save_user_task

extract_product_task >> save_product_task