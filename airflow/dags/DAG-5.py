from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timezone, timedelta
from google.cloud import storage, bigquery
import os
import re
import logging
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET = os.getenv("GCS_BUCKET")
RAW = os.getenv("BQ_RAW_DATASET")
CURATED = os.getenv("BQ_CURATED_DATASET")
LOCATION = os.getenv("BQ_LOCATION")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

PATTERN = r"^invoice_(O\d+)_\d{8}\.pdf$"

def sweep_and_resolve_pdfs():
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    bucket = storage_client.bucket(BUCKET)

    now = datetime.now(timezone.utc)

    # Get unresolved missing-PDF errors
    error_rows = bq_client.query(f"""
        SELECT record_id
        FROM `{PROJECT_ID}.{CURATED}.data_error_logs`
        WHERE error_message = 'Missing Invoice PDF'
          AND resolved_flag = false
    """).result()

    unresolved_orders = {row.record_id for row in error_rows}

    if not unresolved_orders:
        logging.info("No unresolved missing-PDF errors.")
        return

    blobs = list(bucket.list_blobs(prefix="landing/unstructured/"))

    recovered_rows = []
    updates = []
    moves = []

    for blob in blobs:
        file_name = os.path.basename(blob.name)
        match = re.match(PATTERN, file_name)

        if not match:
            continue

        order_id = match.group(1)

        if order_id in unresolved_orders:
            old_path = blob.name
            new_path = f"archive/pdfs/{file_name}"

            recovered_rows.append({
                "order_id": order_id,
                "file_name": file_name,
                "gcs_path": f"gs://{BUCKET}/{new_path}",
                "ingestion_time": now.isoformat()
            })

            updates.append(order_id)
            moves.append((old_path, new_path))

    # Move PDFs
    for old_path, new_path in moves:
        blob = bucket.blob(old_path)
        if blob.exists():
            bucket.rename_blob(blob, new_path)
            logging.info(f"Moved {old_path} → {new_path}")

    # Insert into attachments_raw
    if recovered_rows:
        bq_client.insert_rows_json(
            f"{PROJECT_ID}.{RAW}.attachments_raw",
            recovered_rows
        )

    if updates:
        order_list = ",".join(f"'{o}'" for o in updates)

        # Mark error logs as resolved
        bq_client.query(f"""
            UPDATE `{PROJECT_ID}.{CURATED}.data_error_logs`
            SET resolved_flag = true,
                resolved_at = CURRENT_TIMESTAMP()
            WHERE record_id IN ({order_list})
              AND error_message = 'Missing Invoice PDF'
              AND resolved_flag = false
        """).result()

        # Reset orders_raw status so DAG-2 can certify it
        bq_client.query(f"""
            UPDATE `{PROJECT_ID}.{RAW}.orders_raw`
            SET row_status = 'PASS'
            WHERE order_id IN ({order_list})
        """).result()


with DAG(
    dag_id="dag_5_housekeeping_sweeper",
    start_date=datetime(2026, 1, 19),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    sweep_task = PythonOperator(
        task_id="sweep_and_resolve_pdfs",
        python_callable=sweep_and_resolve_pdfs
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id="trigger_dag_2",
        trigger_dag_id="dag_2_governance_and_audit",
    )

    sweep_task >> trigger_dag_2