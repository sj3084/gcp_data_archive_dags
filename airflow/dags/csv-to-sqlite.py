from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3
import os

DATA_PATH = "/opt/airflow/dags/data"
DB_PATH = "/opt/airflow/dags/data/airflow_pipeline.db"

os.makedirs(DATA_PATH, exist_ok=True)

FILES = {
    "customers": "customers.csv",
    "products": "products.csv",
    "orders": "orders.csv"
}

def infer_sqlite_type(dtype):
    if "int" in str(dtype):
        return "INTEGER"
    if "float" in str(dtype):
        return "REAL"
    if "bool" in str(dtype):
        return "INTEGER"
    return "TEXT"

# ---------------- TASK 1 ----------------
def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for table, file in FILES.items():
        path = os.path.join(DATA_PATH, file)
        df = pd.read_csv(path)

        columns = []
        for col, dtype in df.dtypes.items():
            columns.append(f"{col} {infer_sqlite_type(dtype)}")

        sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"
        cursor.execute(sql)

        print(f"Table ready: {table}")

    conn.commit()
    conn.close()

# ---------------- TASK 2 ----------------
def load_data():
    conn = sqlite3.connect(DB_PATH)

    for table, file in FILES.items():
        path = os.path.join(DATA_PATH, file)
        df = pd.read_csv(path)

        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table}")   # clean reload
        conn.commit()

        df.to_sql(table, conn, if_exists="append", index=False)
        print(f"Loaded data into {table}")

    conn.close()

# ---------------- TASK 3 ----------------
def validate_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for table in FILES.keys():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"{table} row count = {cursor.fetchone()[0]}")

    conn.close()

# ---------------- TASK 4 ----------------
def print_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for table in FILES.keys():
        print(f"\n===== {table.upper()} =====")
        cursor.execute(f"SELECT * FROM {table}")
        for row in cursor.fetchall():
            print(row)

    conn.close()

# ---------------- DAG ----------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="csv_to_sqlite_simple_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    t2 = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    t3 = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    t4 = PythonOperator(
        task_id="print_tables",
        python_callable=print_tables
    )

    t1 >> t2 >> t3 >> t4