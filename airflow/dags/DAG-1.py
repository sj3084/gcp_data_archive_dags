from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from google.cloud import storage, bigquery
import logging
import json
import re
import os
from dotenv import load_dotenv

load_dotenv()

# ==================================================
# CONFIG
# ==================================================
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

FILES = {
    "customers": "customers.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "payments": "payments.csv",
    "returns": "returns.csv",
    "pdf_manifest": "pdf_manifest.csv",
}

# ==================================================
# SCHEMAS (UNCHANGED)
# ==================================================
CUSTOMERS_SCHEMA = [
    {"name": "customer_id", "type": "STRING"},
    {"name": "customer_name", "type": "STRING"},
    {"name": "email", "type": "STRING"},
    {"name": "phone", "type": "STRING"},
    {"name": "created_date", "type": "STRING"},
    {"name": "country", "type": "STRING"},
]

ORDERS_SCHEMA = [
    {"name": "order_id", "type": "STRING"},
    {"name": "customer_id", "type": "STRING"},
    {"name": "order_date", "type": "STRING"},
    {"name": "order_status", "type": "STRING"},
    {"name": "order_total", "type": "STRING"},
    {"name": "source_updated_at", "type": "STRING"},
]

ORDER_ITEMS_SCHEMA = [
    {"name": "order_item_id", "type": "STRING"},
    {"name": "order_id", "type": "STRING"},
    {"name": "product_id", "type": "STRING"},
    {"name": "product_name", "type": "STRING"},
    {"name": "quantity", "type": "STRING"},
    {"name": "unit_price", "type": "STRING"},
]

PAYMENTS_SCHEMA = [
    {"name": "payment_id", "type": "STRING"},
    {"name": "order_id", "type": "STRING"},
    {"name": "payment_date", "type": "STRING"},
    {"name": "payment_method", "type": "STRING"},
    {"name": "amount", "type": "STRING"},
    {"name": "payment_status", "type": "STRING"},
]

RETURNS_SCHEMA = [
    {"name": "return_id", "type": "STRING"},
    {"name": "order_id", "type": "STRING"},
    {"name": "return_date", "type": "STRING"},
    {"name": "reason", "type": "STRING"},
    {"name": "refund_amount", "type": "STRING"},
]

PDF_MANIFEST_SCHEMA = [
    {"name": "file_name", "type": "STRING"},
    {"name": "order_id", "type": "STRING"},
    {"name": "document_type", "type": "STRING"},
    {"name": "created_date", "type": "STRING"},
]

SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "returns": RETURNS_SCHEMA,
    "pdf_manifest": PDF_MANIFEST_SCHEMA,
}

# ==================================================
# HELPERS
# ==================================================
def any_csv_exists():
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    return any(
        blob.name.endswith(".csv")
        for blob in bucket.list_blobs(prefix="landing/structured/")
    )

def csv_for_table_exists(filename):
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    return bucket.blob(f"landing/structured/{filename}").exists()

def move_csvs_logic(**context):
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    dag_start_time = context["dag_run"].logical_date

    for blob in bucket.list_blobs(prefix="landing/structured/"):
        if not blob.name.endswith(".csv"):
            continue

        # Only move files older than DAG start
        if blob.updated < dag_start_time:
            filename = os.path.basename(blob.name)
            base, ext = os.path.splitext(filename)

            ts = blob.updated.strftime("%Y%m%dT%H%M%S")
            dest = f"processed/{base}_{ts}{ext}"

            bucket.rename_blob(blob, dest)

# -------------------------
# PDF PROCESSING LOGIC
# -------------------------
def process_pdfs_logic():
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    bucket = storage_client.bucket(BUCKET)

    # 1. Load valid order IDs from the RAW table (which was just updated by merges)
    orders_query = bq_client.query(
        f"SELECT order_id FROM `{PROJECT_ID}.{RAW}.orders_raw`"
    )
    orders = {row.order_id for row in orders_query.result()}

    # 2. List PDFs
    blobs = list(bucket.list_blobs(prefix="landing/unstructured/"))

    now = datetime.now(timezone.utc)
    pattern = r"^invoice_(O\d+)_\d{8}\.pdf$"

    valid_rows = []
    orphan_rows = []
    error_rows = []
    rename_actions = []  # (old_blob_name, new_path)

    # 3. Define Classification Logic
    def classify_pdf(blob):
        if blob.name.endswith("/") or not blob.name.lower().endswith(".pdf"):
            return None

        file_name = os.path.basename(blob.name)
        match = re.match(pattern, file_name)

        # A. INVALID FILENAME
        if not match:
            dest = f"error/pdfs/{file_name}"
            error = {
                "record_id": file_name,
                "source_table": "pdf_ingest",
                "error_message": "Invalid PDF filename format",
                "raw_data": json.dumps({"file_name": file_name}),
                "retry_count": 0,
                "created_at": now.isoformat(),
                "resolved_flag": False,
                "resolved_at": None,
            }
            return ("error", blob.name, dest, error)

        order_id = match.group(1)

        # B. ORPHAN (Order ID doesn't exist in BQ yet)
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
                "resolved_flag": False,
                "resolved_at": None,
            }
            return ("orphan", blob.name, dest, orphan, error)

        # C. VALID
        dest = f"archive/pdfs/{file_name}"
        valid = {
            "order_id": order_id,
            "file_name": file_name,
            "gcs_path": f"gs://{BUCKET}/{dest}",
            "ingestion_time": now.isoformat(),
        }
        return ("valid", blob.name, dest, valid)

    # 4. Execute Parallel Classification
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

    # 5. Execute Parallel Renames (Moves)
    def rename_blob_pair(pair):
        old_name, dest = pair
        blob = bucket.blob(old_name)
        bucket.rename_blob(blob, dest)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(rename_blob_pair, rename_actions))

    # 6. Bulk Inserts to BigQuery
    if valid_rows:
        bq_client.insert_rows_json(f"{PROJECT_ID}.{RAW}.attachments_raw", valid_rows)
    if orphan_rows:
        bq_client.insert_rows_json(f"{PROJECT_ID}.{CURATED}.orphan_pdfs", orphan_rows)
    if error_rows:
        bq_client.insert_rows_json(f"{PROJECT_ID}.{CURATED}.data_error_logs", error_rows)

# ==================================================
# DAG
# ==================================================
with DAG(
    dag_id="dag_1_ingest_and_link",
    start_date=datetime(2026, 1, 19),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    wait_for_csvs = PythonSensor(
        task_id="wait_for_csvs",
        python_callable=any_csv_exists,
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    # REPLACEMENT FOR SHORTCIRCUITOPERATOR
    def guard(table):
        def _check_and_skip():
            if not csv_for_table_exists(FILES[table]):
                raise AirflowSkipException(f"File {FILES[table]} not found. Skipping {table} pipeline.")
            return True

        return PythonOperator(
            task_id=f"check_{table}",
            python_callable=_check_and_skip,
        )

    def stage_loader(task_id, table):
        return GCSToBigQueryOperator(
            task_id=task_id,
            bucket=BUCKET,
            source_objects=[f"landing/structured/{FILES[table]}"],
            destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.{table}_stage",
            schema_fields=SCHEMAS[table],
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

    # =========================
    # PER-TABLE PIPELINES
    # =========================
    
    # CUSTOMERS
    check_customers = guard("customers")
    load_customers = stage_loader("load_customers_stage", "customers")
    merge_customers = BigQueryInsertJobOperator(
        task_id="merge_customers",
        configuration={"query": {"query": f"""
        MERGE `{PROJECT_ID}.{RAW}.customers_raw` T
        USING `{PROJECT_ID}.{RAW}.customers_stage` S
        ON T.customer_id = S.customer_id
        WHEN MATCHED THEN UPDATE SET
          customer_name = S.customer_name,
          email = S.email,
          phone = S.phone,
          created_date = S.created_date,
          country = S.country,
          ingestion_time = CURRENT_TIMESTAMP(),
          source_file = 'customers.csv'
        WHEN NOT MATCHED THEN
        INSERT (
          customer_id, customer_name, email, phone,
          created_date, country, ingestion_time, source_file
        )
        VALUES (
          S.customer_id, S.customer_name, S.email, S.phone,
          S.created_date, S.country, CURRENT_TIMESTAMP(), 'customers.csv'
        );
        """, "useLegacySql": False}}
    )

    # ORDERS
    check_orders = guard("orders")
    load_orders = stage_loader("load_orders_stage", "orders")
    merge_orders = BigQueryInsertJobOperator(
        task_id="merge_orders",
        configuration={"query": {"query": f"""
        MERGE `{PROJECT_ID}.{RAW}.orders_raw` T
        USING `{PROJECT_ID}.{RAW}.orders_stage` S
        ON T.order_id = S.order_id
        WHEN MATCHED
            AND CAST(S.source_updated_at AS TIMESTAMP)
                > CAST(T.source_updated_at AS TIMESTAMP)
        THEN UPDATE SET
          customer_id = S.customer_id,
          order_date = S.order_date,
          order_status = S.order_status,
          order_total = S.order_total,
          source_updated_at = S.source_updated_at,
          ingestion_time = CURRENT_TIMESTAMP(),
          source_file = 'orders.csv'
        WHEN NOT MATCHED THEN
        INSERT (
          order_id, customer_id, order_date, order_status,
          order_total, source_updated_at, row_status,
          ingestion_time, source_file
        )
        VALUES (
          S.order_id, S.customer_id, S.order_date, S.order_status,
          S.order_total, S.source_updated_at, 'FAIL',
          CURRENT_TIMESTAMP(), 'orders.csv'
        );
        """, "useLegacySql": False}}
    )

    # ORDER ITEMS
    check_items = guard("order_items")
    load_items = stage_loader("load_items_stage", "order_items")
    merge_items = BigQueryInsertJobOperator(
        task_id="merge_order_items",
        configuration={"query": {"query": f"""
        MERGE `{PROJECT_ID}.{RAW}.order_items_raw` T
        USING `{PROJECT_ID}.{RAW}.order_items_stage` S
        ON T.order_item_id = S.order_item_id
        WHEN MATCHED THEN UPDATE SET
          product_name = S.product_name,
          quantity = S.quantity,
          unit_price = S.unit_price,
          ingestion_time = CURRENT_TIMESTAMP(),
          source_file = 'order_items.csv'
        WHEN NOT MATCHED THEN
        INSERT (
          order_item_id, order_id, product_id,
          product_name, quantity, unit_price,
          ingestion_time, source_file
        )
        VALUES (
          S.order_item_id, S.order_id, S.product_id,
          S.product_name, S.quantity, S.unit_price,
          CURRENT_TIMESTAMP(), 'order_items.csv'
        );
        """, "useLegacySql": False}}
    )

    # PAYMENTS
    check_payments = guard("payments")
    load_payments = stage_loader("load_payments_stage", "payments")
    merge_payments = BigQueryInsertJobOperator(
        task_id="merge_payments",
        configuration={"query": {"query": f"""
        MERGE `{PROJECT_ID}.{RAW}.payments_raw` T
        USING `{PROJECT_ID}.{RAW}.payments_stage` S
        ON T.payment_id = S.payment_id
        WHEN MATCHED THEN UPDATE SET
          order_id = S.order_id,
          payment_date = S.payment_date,
          payment_method = S.payment_method,
          amount = S.amount,
          payment_status = S.payment_status,
          ingestion_time = CURRENT_TIMESTAMP(),
          source_file = 'payments.csv'
        WHEN NOT MATCHED THEN
        INSERT (
          payment_id, order_id, payment_date,
          payment_method, amount, payment_status,
          ingestion_time, source_file
        )
        VALUES (
          S.payment_id, S.order_id, S.payment_date,
          S.payment_method, S.amount, S.payment_status,
          CURRENT_TIMESTAMP(), 'payments.csv'
        );
        """, "useLegacySql": False}}
    )

    # RETURNS
    check_returns = guard("returns")
    load_returns = stage_loader("load_returns_stage", "returns")
    merge_returns = BigQueryInsertJobOperator(
        task_id="merge_returns",
        configuration={"query": {"query": f"""
        MERGE `{PROJECT_ID}.{RAW}.returns_raw` T
        USING `{PROJECT_ID}.{RAW}.returns_stage` S
        ON T.return_id = S.return_id
        WHEN MATCHED THEN UPDATE SET
          order_id = S.order_id,
          return_date = S.return_date,
          reason = S.reason,
          refund_amount = S.refund_amount,
          ingestion_time = CURRENT_TIMESTAMP(),
          source_file = 'returns.csv'
        WHEN NOT MATCHED THEN
        INSERT (
          return_id, order_id, return_date,
          reason, refund_amount, ingestion_time, source_file
        )
        VALUES (
          S.return_id, S.order_id, S.return_date,
          S.reason, S.refund_amount, CURRENT_TIMESTAMP(), 'returns.csv'
        );
        """, "useLegacySql": False}}
    )

    # PDF MANIFEST
    check_pdf = guard("pdf_manifest")
    load_pdf = stage_loader("load_pdf_manifest_stage", "pdf_manifest")
    merge_pdf = BigQueryInsertJobOperator(
        task_id="merge_pdf_manifest",
        configuration={"query": {"query": f"""
        MERGE `{PROJECT_ID}.{RAW}.pdf_manifest_raw` T
        USING `{PROJECT_ID}.{RAW}.pdf_manifest_stage` S
        ON T.file_name = S.file_name
        WHEN MATCHED THEN UPDATE SET
          order_id = S.order_id,
          document_type = S.document_type,
          created_date = S.created_date,
          ingestion_time = CURRENT_TIMESTAMP(),
          source_file = 'pdf_manifest.csv'
        WHEN NOT MATCHED THEN
        INSERT (
          file_name, order_id, document_type,
          created_date, ingestion_time, source_file
        )
        VALUES (
          S.file_name, S.order_id, S.document_type,
          S.created_date, CURRENT_TIMESTAMP(), 'pdf_manifest.csv'
        );
        """, "useLegacySql": False}}
    )

    move_csvs = PythonOperator(
        task_id="move_csvs",
        python_callable=move_csvs_logic,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    process_pdfs = PythonOperator(
        task_id="process_pdfs",
        python_callable=process_pdfs_logic,
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id="trigger_dag_2",
        trigger_dag_id="dag_2_governance_and_audit",
    )

    # DEPENDENCIES
    wait_for_csvs >> check_customers >> load_customers >> merge_customers
    wait_for_csvs >> check_orders >> load_orders >> merge_orders
    wait_for_csvs >> check_items >> load_items >> merge_items
    wait_for_csvs >> check_payments >> load_payments >> merge_payments
    wait_for_csvs >> check_returns >> load_returns >> merge_returns
    wait_for_csvs >> check_pdf >> load_pdf >> merge_pdf

    # After merges are done, move the processed CSVs
    [
        merge_customers,
        merge_orders,
        merge_items,
        merge_payments,
        merge_returns,
        merge_pdf,
    ] >> move_csvs 
    
    # After CSVs are moved (and Orders BQ table is updated), process PDFs, then trigger DAG 2
    move_csvs >> process_pdfs >> trigger_dag_2