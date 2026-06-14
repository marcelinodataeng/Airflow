from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="postgres_ingestion",
    start_date=datetime(2026, 6, 14),
    schedule=None
)

def extract_user():
    pass

def save_user():
    pass

def extract_product():
    pass
def save_product():
    pass