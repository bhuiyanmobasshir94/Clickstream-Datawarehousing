import os
import sys
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

PARENT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(PARENT_DIR)

# from config import *
from create_datalake_dwh import (
    create_datalake,
    create_tables,
    extract_transform_load,
    populate_datacatalog,
)

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="clickstream",
    description="Datalake to datawarehouse creation",
    default_args=default_args,
    # schedule_interval="@daily",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["clickstream", "dwh", "datalake"],
) as dag:
    create_tables = PythonOperator(
        task_id="create_tables", python_callable=create_tables,
    )

    populate_datacatalog = PythonOperator(
        task_id="populate_datacatalog", python_callable=populate_datacatalog,
    )

    create_datalake = PythonOperator(
        task_id="create_datalake", python_callable=create_datalake,
    )

    extract_transform_load = PythonOperator(
        task_id="extract_transform_load", python_callable=extract_transform_load,
    )

    create_tables >> populate_datacatalog >> create_datalake >> extract_transform_load

