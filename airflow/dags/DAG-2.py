from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

PROJECT_ID = os.getenv("PROJECT_ID")
RAW = os.getenv("BQ_RAW_DATASET")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_2_governance_and_audit",
    start_date=datetime(2026, 1, 19),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    update_order_status = BigQueryInsertJobOperator(
        task_id="update_order_status",
        configuration={
            "query": {
                "query": f"""
                -- Mark PASS where attachment exists
                UPDATE `{PROJECT_ID}.{RAW}.orders_raw` o
                SET row_status = 'PASS'
                WHERE EXISTS (
                    SELECT 1
                    FROM `{PROJECT_ID}.{RAW}.attachments_raw` a
                    WHERE a.order_id = o.order_id
                );

                -- Mark FAIL where attachment does NOT exist
                UPDATE `{PROJECT_ID}.{RAW}.orders_raw` o
                SET row_status = 'FAIL'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM `{PROJECT_ID}.{RAW}.attachments_raw` a
                    WHERE a.order_id = o.order_id
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    trigger_dag_3 = TriggerDagRunOperator(
        task_id="trigger_dag_3",
        trigger_dag_id="dag_3_curation",
    )

    update_order_status >> trigger_dag_3