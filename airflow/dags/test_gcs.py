from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime

with DAG(
    "test_gcs_connection",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:

    list_files = GCSListObjectsOperator(
        task_id="list_files",
        bucket="archive-demo-bucket"
    )