from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from google.cloud import storage, bigquery
import logging
import json
import os
import re

# -------------------------
# CONFIG
# -------------------------
PROJECT_ID = "archive-demo-project-484906"
BUCKET = "archive-demo-bucket"
RAW = "raw_dataset"
CURATED = "curated_dataset"
LOCATION = "europe-west2"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# -------------------------
# CSV MOVE
# -------------------------
def move_csvs_logic():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)

    for blob in bucket.list_blobs(prefix="landing/structured/"):
        if blob.name.endswith("/") or not blob.name.endswith(".csv"):
            continue
        dest = f"processed/{os.path.basename(blob.name)}"
        bucket.rename_blob(blob, dest)
        logging.info(f"Moved CSV to {dest}")

# -------------------------
# PDF PROCESSING (FINAL)
# -------------------------
def process_pdfs_logic():
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    bucket = storage_client.bucket(BUCKET)

    # Load valid order IDs
    orders_query = bq_client.query(
        f"SELECT order_id FROM `{PROJECT_ID}.{RAW}.orders_raw`"
    )
    orders = {row.order_id for row in orders_query.result()}

    blobs = list(bucket.list_blobs(prefix="landing/unstructured/"))

    now = datetime.now(timezone.utc)
    pattern = r"^invoice_(O\d+)_\d{8}\.pdf$"

    valid_rows = []
    orphan_rows = []
    error_rows = []
    rename_actions = []  # (old_blob_name, new_path)

    def classify_pdf(blob):
        if blob.name.endswith("/") or not blob.name.lower().endswith(".pdf"):
            return None

        file_name = os.path.basename(blob.name)
        match = re.match(pattern, file_name)

        # ---------- INVALID FILENAME ----------
        if not match:
            dest = f"error/pdfs/{file_name}"
            error = {
                "record_id": file_name,
                "source_table": "pdf_ingest",
                "error_message": "Invalid PDF filename format",
                "raw_data": json.dumps({"file_name": file_name}),
                "retry_count": 0,
                "created_at": now.isoformat(),
            }
            return ("error", blob.name, dest, error)

        order_id = match.group(1)

        # ---------- ORPHAN ----------
        if order_id not in orders:
            dest = f"error/pdfs/{file_name}"

            orphan = {
                "file_name": file_name,
                "order_id": order_id,
                "gcs_path": f"gs://{BUCKET}/{dest}",
                "detected_at": now.isoformat(),
                "reason": "Order ID not found",
            }

            error = {
                "record_id": order_id,
                "source_table": "orphan_pdfs",
                "error_message": "Order ID missing in Orders",
                "raw_data": json.dumps({"file_name": file_name, "order_id": order_id}),
                "retry_count": 0,
                "created_at": now.isoformat(),
            }

            return ("orphan", blob.name, dest, orphan, error)

        # ---------- VALID ----------
        dest = f"archive/pdfs/{file_name}"

        valid = {
            "order_id": order_id,
            "file_name": file_name,
            "gcs_path": f"gs://{BUCKET}/{dest}",
            "ingestion_time": now.isoformat(),
        }

        return ("valid", blob.name, dest, valid)

    # -------- PARALLEL CLASSIFICATION --------
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(classify_pdf, blob) for blob in blobs]

        for future in as_completed(futures):
            result = future.result()
            if not result:
                continue

            kind = result[0]

            if kind == "valid":
                _, old_name, dest, valid = result
                valid_rows.append(valid)
                rename_actions.append((old_name, dest))

            elif kind == "orphan":
                _, old_name, dest, orphan, error = result
                orphan_rows.append(orphan)
                error_rows.append(error)
                rename_actions.append((old_name, dest))

            elif kind == "error":
                _, old_name, dest, error = result
                error_rows.append(error)
                rename_actions.append((old_name, dest))

    # -------- SERIAL RENAMES (SAFE) --------
    for old_name, dest in rename_actions:
        blob = bucket.blob(old_name)
        bucket.rename_blob(blob, dest)

    # -------- BQ INSERTS --------
    if valid_rows:
        bq_client.insert_rows_json(
            f"{PROJECT_ID}.{RAW}.attachments_raw",
            valid_rows
        )

    if orphan_rows:
        bq_client.insert_rows_json(
            f"{PROJECT_ID}.{CURATED}.orphan_pdfs",
            orphan_rows
        )

    if error_rows:
        bq_client.insert_rows_json(
            f"{PROJECT_ID}.{CURATED}.data_error_logs",
            error_rows
        )

# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="dag_1_ingest_and_link",
    start_date=datetime(2026, 1, 19),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    load_customers = GCSToBigQueryOperator(
        task_id="load_customers",
        bucket=BUCKET,
        source_objects=["landing/structured/customers.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.customers_raw",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_orders = GCSToBigQueryOperator(
        task_id="load_orders",
        bucket=BUCKET,
        source_objects=["landing/structured/orders.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.orders_raw",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_items = GCSToBigQueryOperator(
        task_id="load_items",
        bucket=BUCKET,
        source_objects=["landing/structured/order_items.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.order_items_raw",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_payments = GCSToBigQueryOperator(
        task_id="load_payments",
        bucket=BUCKET,
        source_objects=["landing/structured/payments.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.payments_raw",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_returns = GCSToBigQueryOperator(
        task_id="load_returns",
        bucket=BUCKET,
        source_objects=["landing/structured/returns.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.returns_raw",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_pdf_manifest = GCSToBigQueryOperator(
        task_id="load_pdf_manifest",
        bucket=BUCKET,
        source_objects=["landing/structured/pdf_manifest.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.pdf_manifest_raw",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    normalize = BigQueryInsertJobOperator(
        task_id="normalize_raw",
        configuration={
            "query": {
                "query": f"""
                -- ORDERS
                ALTER TABLE `{PROJECT_ID}.{RAW}.orders_raw`
                ADD COLUMN IF NOT EXISTS row_status STRING;

                ALTER TABLE `{PROJECT_ID}.{RAW}.orders_raw`
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;

                UPDATE `{PROJECT_ID}.{RAW}.orders_raw`
                SET row_status = COALESCE(row_status,'FAIL'),
                    ingestion_time = COALESCE(ingestion_time,CURRENT_TIMESTAMP())
                WHERE TRUE;

                -- CUSTOMERS
                ALTER TABLE `{PROJECT_ID}.{RAW}.customers_raw`
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;

                UPDATE `{PROJECT_ID}.{RAW}.customers_raw`
                SET ingestion_time = COALESCE(ingestion_time,CURRENT_TIMESTAMP())
                WHERE TRUE;

                -- ITEMS
                ALTER TABLE `{PROJECT_ID}.{RAW}.order_items_raw`
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;

                UPDATE `{PROJECT_ID}.{RAW}.order_items_raw`
                SET ingestion_time = COALESCE(ingestion_time,CURRENT_TIMESTAMP())
                WHERE TRUE;

                -- PAYMENTS
                ALTER TABLE `{PROJECT_ID}.{RAW}.payments_raw`
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;

                UPDATE `{PROJECT_ID}.{RAW}.payments_raw`
                SET ingestion_time = COALESCE(ingestion_time,CURRENT_TIMESTAMP())
                WHERE TRUE;

                -- RETURNS
                ALTER TABLE `{PROJECT_ID}.{RAW}.returns_raw`
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;

                UPDATE `{PROJECT_ID}.{RAW}.returns_raw`
                SET ingestion_time = COALESCE(ingestion_time,CURRENT_TIMESTAMP())
                WHERE TRUE;

                -- PDF MANIFEST
                ALTER TABLE `{PROJECT_ID}.{RAW}.pdf_manifest_raw`
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;

                UPDATE `{PROJECT_ID}.{RAW}.pdf_manifest_raw`
                SET ingestion_time = COALESCE(ingestion_time,CURRENT_TIMESTAMP())
                WHERE TRUE;

                -- ATTACHMENTS
                ALTER TABLE `{PROJECT_ID}.{RAW}.attachments_raw`
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;

                UPDATE `{PROJECT_ID}.{RAW}.attachments_raw`
                SET ingestion_time = COALESCE(ingestion_time,CURRENT_TIMESTAMP())
                WHERE TRUE;
                """,
                "useLegacySql": False,
            }
        },
    )

    move_csv = PythonOperator(
        task_id="move_csvs",
        python_callable=move_csvs_logic
    )

    process_pdfs = PythonOperator(
        task_id="process_pdfs",
        python_callable=process_pdfs_logic
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id="trigger_dag_2",
        trigger_dag_id="dag_2_governance_and_audit",
    )

    [load_customers, load_orders, load_items, load_payments, load_returns, load_pdf_manifest] >> normalize
    normalize >> move_csv >> process_pdfs >> trigger_dag_2