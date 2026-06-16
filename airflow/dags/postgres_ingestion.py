from airflow.providers.postgres.hooks.postgres import PostgresHook
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

        usuario = hook.get_pandas_df(
            "SELECT * FROM usuario"
        )
        return usuario

    except Exception as e:
        raise e

def save_user(usuarios_df: pd.DataFrame):
    usuarios_df.to_csv("landing/usuarios.csv",
    index=False
    )

def extract_product() -> pd.DataFrame:
    try:
        hook = PostgresHook(
            postgres_conn_id="postgres_default"
        )

        produtos = hook.get_pandas_df(
            "SELECT * FROM produto"
        )
        return produtos

    except Exception as e:
        raise e

def save_product(produtos_df: pd.DataFrame):
    produtos_df.to_csv("landing/produtos.csv",
    index=False)
