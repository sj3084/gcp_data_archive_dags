from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET = os.getenv("GCS_BUCKET")
CURATED = os.getenv("BQ_CURATED_DATASET")
BUSINESS = os.getenv("BQ_BUSINESS_CURATED_DATASET")
LOCATION = os.getenv("BQ_LOCATION")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_4_business_aggregations",
    start_date=datetime(2026, 1, 19),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    # -------------------------
    # 1. CUSTOMER CLV
    # -------------------------
    build_customer_clv = BigQueryInsertJobOperator(
        task_id="build_customer_clv",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{BUSINESS}.customer_clv`;

                INSERT INTO `{PROJECT_ID}.{BUSINESS}.customer_clv`
                SELECT
                  c.customer_id,
                  COUNT(o.order_id) AS total_orders,
                  SUM(o.order_total) AS total_spend,
                  AVG(o.order_total) AS avg_order_value,
                  MIN(o.order_date) AS first_order_date,
                  MAX(o.order_date) AS last_order_date,
                  DATE_DIFF(MAX(o.order_date), MIN(o.order_date), DAY) AS customer_lifetime_days,
                  SUM(o.order_total) AS clv_amount
                FROM `{PROJECT_ID}.{CURATED}.customers_curated` c
                JOIN `{PROJECT_ID}.{CURATED}.orders_curated` o
                  ON c.customer_id = o.customer_id
                GROUP BY c.customer_id;
                """,
                "useLegacySql": False,
            }
        },
    )

    # -------------------------
    # 2. CUSTOMER SEGMENTS
    # -------------------------
    build_customer_segments = BigQueryInsertJobOperator(
        task_id="build_customer_segments",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{BUSINESS}.customer_segments`;

                INSERT INTO `{PROJECT_ID}.{BUSINESS}.customer_segments`
                SELECT
                  customer_id,
                  clv_amount,
                  total_orders,
                  CASE
                    WHEN clv_amount > 50000 THEN 'HIGH_VALUE'
                    WHEN clv_amount BETWEEN 20000 AND 50000 THEN 'MEDIUM_VALUE'
                    ELSE 'LOW_VALUE'
                  END AS customer_segment
                FROM `{PROJECT_ID}.{BUSINESS}.customer_clv`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # -------------------------
    # 3. MONTHLY SALES
    # -------------------------
    build_monthly_sales = BigQueryInsertJobOperator(
        task_id="build_monthly_sales",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{BUSINESS}.monthly_sales`;

                INSERT INTO `{PROJECT_ID}.{BUSINESS}.monthly_sales`
                SELECT
                  EXTRACT(YEAR FROM order_date) AS year,
                  EXTRACT(MONTH FROM order_date) AS month,
                  COUNT(order_id) AS total_orders,
                  SUM(order_total) AS total_revenue,
                  AVG(order_total) AS avg_order_value
                FROM `{PROJECT_ID}.{CURATED}.orders_curated`
                GROUP BY year, month;
                """,
                "useLegacySql": False,
            }
        },
    )

    # -------------------------
    # 4. PRODUCT PERFORMANCE
    # -------------------------
    build_product_performance = BigQueryInsertJobOperator(
        task_id="build_product_performance",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{BUSINESS}.product_performance`;

                INSERT INTO `{PROJECT_ID}.{BUSINESS}.product_performance`
                SELECT
                  product_id,
                  SUM(quantity) AS total_quantity_sold,
                  SUM(quantity * unit_price) AS total_revenue,
                  COUNT(DISTINCT order_id) AS total_orders
                FROM `{PROJECT_ID}.{CURATED}.order_items_curated`
                GROUP BY product_id;
                """,
                "useLegacySql": False,
            }
        },
    )

    # -------------------------
    # 5. PRODUCT RETURNS
    # -------------------------
    build_product_returns = BigQueryInsertJobOperator(
        task_id="build_product_returns",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                TRUNCATE TABLE `{PROJECT_ID}.{BUSINESS}.product_returns`;

                INSERT INTO `{PROJECT_ID}.{BUSINESS}.product_returns`
                WITH sold AS (
                  SELECT
                    product_id,
                    SUM(quantity) AS total_sold
                  FROM `{PROJECT_ID}.{CURATED}.order_items_curated`
                  GROUP BY product_id
                ),
                returned AS (
                  SELECT
                    oi.product_id,
                    COUNT(r.return_id) AS total_returned
                  FROM `{PROJECT_ID}.{CURATED}.returns_curated` r
                  JOIN `{PROJECT_ID}.{CURATED}.order_items_curated` oi
                    ON r.order_id = oi.order_id
                  GROUP BY oi.product_id
                )
                SELECT
                  s.product_id,
                  s.total_sold,
                  COALESCE(r.total_returned, 0) AS total_returned,
                  SAFE_DIVIDE(COALESCE(r.total_returned,0), s.total_sold) AS return_rate
                FROM sold s
                LEFT JOIN returned r
                  ON s.product_id = r.product_id;
                """,
                "useLegacySql": False,
            }
        },
    )

    # -------------------------
    # FLOW
    # -------------------------
    build_customer_clv >> build_customer_segments
    build_customer_segments >> build_monthly_sales
    build_monthly_sales >> build_product_performance
    build_product_performance >> build_product_returns