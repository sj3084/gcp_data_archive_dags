from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
RAW = os.getenv("BQ_RAW_DATASET")
CURATED = os.getenv("BQ_CURATED_DATASET")
LOCATION = os.getenv("BQ_LOCATION")

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
                SELECT
                    customer_id,
                    customer_name,
                    email,
                    CAST(phone AS STRING),
                    parsed_created_date AS created_date,
                    country,
                    ingestion_time
                FROM (
                    SELECT *,
                        CASE
                            WHEN REGEXP_CONTAINS(created_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                                THEN PARSE_DATE('%d-%m-%Y', created_date)
                            WHEN REGEXP_CONTAINS(created_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                                THEN PARSE_DATE('%Y-%m-%d', created_date)
                            ELSE NULL
                        END AS parsed_created_date
                    FROM `{PROJECT_ID}.{RAW}.customers_raw`
                )
                WHERE parsed_created_date IS NOT NULL;

                INSERT INTO `{PROJECT_ID}.{CURATED}.data_error_logs`
                SELECT
                    customer_id,
                    'customers_raw',
                    'Invalid created_date format',
                    TO_JSON(c),
                    0,
                    CURRENT_TIMESTAMP(),
                    FALSE,
                    NULL
                FROM `{PROJECT_ID}.{RAW}.customers_raw` c
                WHERE created_date IS NOT NULL
                  AND NOT (
                    REGEXP_CONTAINS(created_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                    OR REGEXP_CONTAINS(created_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                  );
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

                INSERT INTO `{PROJECT_ID}.{CURATED}.orders_curated`
                SELECT
                    order_id,
                    customer_id,
                    parsed_order_date AS order_date,
                    order_status,
                    CAST(order_total AS FLOAT64),
                    CAST(source_updated_at AS TIMESTAMP),
                    row_status,
                    ingestion_time
                FROM (
                    SELECT *,
                        CASE
                            WHEN REGEXP_CONTAINS(order_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                                THEN PARSE_DATE('%d-%m-%Y', order_date)
                            WHEN REGEXP_CONTAINS(order_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                                THEN PARSE_DATE('%Y-%m-%d', order_date)
                            ELSE NULL
                        END AS parsed_order_date
                    FROM `{PROJECT_ID}.{RAW}.orders_raw`
                    WHERE row_status = 'PASS'
                )
                WHERE parsed_order_date IS NOT NULL;

                INSERT INTO `{PROJECT_ID}.{CURATED}.data_error_logs`
                SELECT
                    order_id,
                    'orders_raw',
                    'Invalid order_date format',
                    TO_JSON(o),
                    0,
                    CURRENT_TIMESTAMP(),
                    FALSE,
                    NULL
                FROM `{PROJECT_ID}.{RAW}.orders_raw` o
                WHERE row_status = 'PASS'
                  AND order_date IS NOT NULL
                  AND NOT (
                    REGEXP_CONTAINS(order_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                    OR REGEXP_CONTAINS(order_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                  );

                INSERT INTO `{PROJECT_ID}.{CURATED}.data_error_logs`
                SELECT
                    order_id,
                    'orders_raw',
                    'Missing Invoice PDF',
                    TO_JSON(o),
                    0,
                    CURRENT_TIMESTAMP(),
                    FALSE,
                    NULL
                FROM `{PROJECT_ID}.{RAW}.orders_raw` o
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
                    parsed_payment_date AS payment_date,
                    payment_method,
                    CAST(amount AS FLOAT64),
                    payment_status,
                    ingestion_time
                FROM (
                    SELECT *,
                        CASE
                            WHEN REGEXP_CONTAINS(payment_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                                THEN PARSE_DATE('%d-%m-%Y', payment_date)
                            WHEN REGEXP_CONTAINS(payment_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                                THEN PARSE_DATE('%Y-%m-%d', payment_date)
                            ELSE NULL
                        END AS parsed_payment_date
                    FROM `{PROJECT_ID}.{RAW}.payments_raw`
                )
                WHERE parsed_payment_date IS NOT NULL
                  AND order_id IN (
                    SELECT order_id FROM `{PROJECT_ID}.{CURATED}.orders_curated`
                  );

                INSERT INTO `{PROJECT_ID}.{CURATED}.data_error_logs`
                SELECT
                    payment_id,
                    'payments_raw',
                    'Invalid payment_date format',
                    TO_JSON(p),
                    0,
                    CURRENT_TIMESTAMP(),
                    FALSE,
                    NULL
                FROM `{PROJECT_ID}.{RAW}.payments_raw` p
                WHERE payment_date IS NOT NULL
                  AND NOT (
                    REGEXP_CONTAINS(payment_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                    OR REGEXP_CONTAINS(payment_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
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
                    parsed_return_date AS return_date,
                    reason,
                    CAST(refund_amount AS FLOAT64),
                    ingestion_time
                FROM (
                    SELECT *,
                        CASE
                            WHEN REGEXP_CONTAINS(return_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                                THEN PARSE_DATE('%d-%m-%Y', return_date)
                            WHEN REGEXP_CONTAINS(return_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                                THEN PARSE_DATE('%Y-%m-%d', return_date)
                            ELSE NULL
                        END AS parsed_return_date
                    FROM `{PROJECT_ID}.{RAW}.returns_raw`
                )
                WHERE parsed_return_date IS NOT NULL
                  AND order_id IN (
                    SELECT order_id FROM `{PROJECT_ID}.{CURATED}.orders_curated`
                  );

                INSERT INTO `{PROJECT_ID}.{CURATED}.data_error_logs`
                SELECT
                    return_id,
                    'returns_raw',
                    'Invalid return_date format',
                    TO_JSON(r),
                    0,
                    CURRENT_TIMESTAMP(),
                    FALSE,
                    NULL
                FROM `{PROJECT_ID}.{RAW}.returns_raw` r
                WHERE return_date IS NOT NULL
                  AND NOT (
                    REGEXP_CONTAINS(return_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                    OR REGEXP_CONTAINS(return_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
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
                    parsed_created_date AS created_date,
                    ingestion_time
                FROM (
                    SELECT *,
                        CASE
                            WHEN REGEXP_CONTAINS(created_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                                THEN PARSE_DATE('%d-%m-%Y', created_date)
                            WHEN REGEXP_CONTAINS(created_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                                THEN PARSE_DATE('%Y-%m-%d', created_date)
                            ELSE NULL
                        END AS parsed_created_date
                    FROM `{PROJECT_ID}.{RAW}.pdf_manifest_raw`
                )
                WHERE parsed_created_date IS NOT NULL;

                INSERT INTO `{PROJECT_ID}.{CURATED}.data_error_logs`
                SELECT
                    file_name,
                    'pdf_manifest_raw',
                    'Invalid created_date format',
                    TO_JSON(p),
                    0,
                    CURRENT_TIMESTAMP(),
                    FALSE,
                    NULL
                FROM `{PROJECT_ID}.{RAW}.pdf_manifest_raw` p
                WHERE created_date IS NOT NULL
                  AND NOT (
                    REGEXP_CONTAINS(created_date, r'^\\d{{2}}-\\d{{2}}-\\d{{4}}$')
                    OR REGEXP_CONTAINS(created_date, r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                  );
                """,
                "useLegacySql": False,
            }
        },
    )

    trigger_dag_4 = TriggerDagRunOperator(
        task_id="trigger_dag_4",
        trigger_dag_id="dag_4_business_aggregations",
    )

    curate_customers >> curate_orders
    curate_orders >> [
        curate_items,
        curate_payments,
        curate_returns,
        curate_attachments,
        curate_pdf_manifest,
    ]
    [curate_items, curate_payments, curate_returns, curate_attachments, curate_pdf_manifest] >> trigger_dag_4