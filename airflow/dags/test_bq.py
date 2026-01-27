from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    "test_bigquery_connection",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_bq = BigQueryInsertJobOperator(
        task_id="test_bq",
        configuration={
            "query": {
                "query": "SELECT CURRENT_TIMESTAMP()",
                "useLegacySql": False
            }
        }
    )
