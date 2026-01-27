from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

PROJECT_ID = "archive-demo-project-484906"
RAW = "raw_dataset"
CURATED = "curated_dataset"
LOCATION = "europe-west2"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_3_curation",
    start_date=datetime(2026, 1, 19),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    # =========================================================
    # 1. CUSTOMERS
    # =========================================================
    curate_customers = BigQueryInsertJobOperator(
        task_id="curate_customers",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{CURATED}.customers_curated`;

                INSERT INTO `{PROJECT_ID}.{CURATED}.customers_curated`
                (customer_id, customer_name, email, phone, created_date, country, ingestion_time)
                SELECT
                    customer_id,
                    customer_name,
                    email,
                    CAST(phone AS STRING),
                    CAST(created_date AS DATE),
                    country,
                    ingestion_time
                FROM `{PROJECT_ID}.{RAW}.customers_raw`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================================================
    # 2. ORDERS (GATEKEEPER)
    # =========================================================
    curate_orders = BigQueryInsertJobOperator(
        task_id="curate_orders",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{CURATED}.orders_curated`;

                -- PASS orders only
                INSERT INTO `{PROJECT_ID}.{CURATED}.orders_curated`
                (order_id, customer_id, order_date, order_status, order_total,
                 source_updated_at, row_status, ingestion_time)
                SELECT
                    order_id,
                    customer_id,
                    CAST(order_date AS DATE),
                    order_status,
                    CAST(order_total AS FLOAT64),
                    CAST(source_updated_at AS TIMESTAMP),
                    row_status,
                    ingestion_time
                FROM `{PROJECT_ID}.{RAW}.orders_raw`
                WHERE row_status = 'PASS';

                -- FAIL orders go to error logs
                INSERT INTO `{PROJECT_ID}.{CURATED}.data_error_logs`
                (record_id, source_table, error_message, raw_data, retry_count, created_at)
                SELECT
                    order_id,
                    'orders_raw',
                    'Missing Invoice PDF',
                    TO_JSON(t),
                    0,
                    CURRENT_TIMESTAMP()
                FROM `{PROJECT_ID}.{RAW}.orders_raw` t
                WHERE row_status = 'FAIL';
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================================================
    # 3. ORDER ITEMS
    # =========================================================
    curate_items = BigQueryInsertJobOperator(
        task_id="curate_items",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{CURATED}.order_items_curated`;

                INSERT INTO `{PROJECT_ID}.{CURATED}.order_items_curated`
                SELECT
                    order_item_id,
                    order_id,
                    product_id,
                    product_name,
                    CAST(quantity AS INT64),
                    CAST(unit_price AS FLOAT64),
                    ingestion_time
                FROM `{PROJECT_ID}.{RAW}.order_items_raw`
                WHERE order_id IN (
                    SELECT order_id FROM `{PROJECT_ID}.{CURATED}.orders_curated`
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================================================
    # 4. PAYMENTS
    # =========================================================
    curate_payments = BigQueryInsertJobOperator(
        task_id="curate_payments",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{CURATED}.payments_curated`;

                INSERT INTO `{PROJECT_ID}.{CURATED}.payments_curated`
                SELECT
                    payment_id,
                    order_id,
                    CAST(payment_date AS DATE),
                    payment_method,
                    CAST(amount AS FLOAT64),
                    payment_status,
                    ingestion_time
                FROM `{PROJECT_ID}.{RAW}.payments_raw`
                WHERE order_id IN (
                    SELECT order_id FROM `{PROJECT_ID}.{CURATED}.orders_curated`
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================================================
    # 5. RETURNS
    # =========================================================
    curate_returns = BigQueryInsertJobOperator(
        task_id="curate_returns",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{CURATED}.returns_curated`;

                INSERT INTO `{PROJECT_ID}.{CURATED}.returns_curated`
                SELECT
                    return_id,
                    order_id,
                    CAST(return_date AS DATE),
                    reason,
                    CAST(refund_amount AS FLOAT64),
                    ingestion_time
                FROM `{PROJECT_ID}.{RAW}.returns_raw`
                WHERE order_id IN (
                    SELECT order_id FROM `{PROJECT_ID}.{CURATED}.orders_curated`
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================================================
    # 6. ATTACHMENTS
    # =========================================================
    curate_attachments = BigQueryInsertJobOperator(
        task_id="curate_attachments",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{CURATED}.attachments_curated`;

                INSERT INTO `{PROJECT_ID}.{CURATED}.attachments_curated`
                SELECT
                    order_id,
                    file_name,
                    gcs_path,
                    ingestion_time
                FROM `{PROJECT_ID}.{RAW}.attachments_raw`
                WHERE order_id IN (
                    SELECT order_id FROM `{PROJECT_ID}.{CURATED}.orders_curated`
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================================================
    # 7. PDF MANIFEST
    # =========================================================
    curate_pdf_manifest = BigQueryInsertJobOperator(
        task_id="curate_pdf_manifest",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{CURATED}.pdf_manifest_curated`;

                INSERT INTO `{PROJECT_ID}.{CURATED}.pdf_manifest_curated`
                SELECT
                    file_name,
                    order_id,
                    document_type,
                    CAST(created_date AS DATE),
                    ingestion_time
                FROM `{PROJECT_ID}.{RAW}.pdf_manifest_raw`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================================================
    # TRIGGER DAG-4
    # =========================================================
    trigger_dag_4 = TriggerDagRunOperator(
        task_id="trigger_dag_4",
        trigger_dag_id="dag_4_business_aggregations",
    )

    # =========================================================
    # FLOW
    # =========================================================
    curate_customers >> curate_orders
    curate_orders >> [
        curate_items,
        curate_payments,
        curate_returns,
        curate_attachments,
        curate_pdf_manifest,
    ]
    [curate_items, curate_payments, curate_returns, curate_attachments, curate_pdf_manifest] >> trigger_dag_4