# GCP Data Archival & Retrieval Platform

**Compliance-Driven Data Engineering Pipeline**

This project implements an enterprise-grade data archival, governance, and analytics platform on **Google Cloud Platform (GCP)**. It ingests structured business data (CSV) and unstructured evidence (PDF invoices), links them dynamically, and enforces strict compliance rules before promoting data to analytics layers.

---

## 📌 Business Rule
> **"No Order is valid without physical proof (Invoice PDF)."**

Only orders with verified invoice PDFs are promoted to the **Golden Record (Curated Layer)**.

---

## 🧩 Key Features
* ✅ **Hybrid Ingestion:** Handles both Structured (CSV) and Unstructured (PDF) data types.
* 🛡️ **Gatekeeper Architecture:** Enforces compliance by quarantining data without physical proof.
* 🗂️ **Zoned Data Lake:** Organized into `Landing`, `Processed`, `Archive`, and `Error` zones.
* 📊 **Analytics-Ready:** Produces clean, curated datasets for BI consumption.
* 🔐 **Production Security:** Uses IAM Service Accounts (Least Privilege) instead of personal credentials.
* 🔁 **Orchestration:** Fully automated using **Apache Airflow** (4 Sequential DAGs).
* 📈 **Business Insights:** Derives CLV, segmentation, and product performance metrics.

---

## 🏗️ Architecture Overview

### 1. Storage Layer (Google Cloud Storage)
The Data Lake is zoned for lifecycle management:
* `landing/structured/` → Raw CSV files (Orders, Customers, etc.)
* `landing/unstructured/` → Raw PDF invoices
* `processed/` → Archived/Processed CSVs
* `archive/pdfs/` → **Validated** Evidence (Linked to Orders)
* `error/pdfs/` → **Orphan** or Invalid PDFs (Quarantine)

### 2. Compute & Warehouse (BigQuery)
The data warehouse is stratified into three layers:

#### **A. Raw Dataset (`raw_dataset`)**
*Staging layer with loose schema (Strings).*
* **Tables:** `customers_raw`, `orders_raw`, `order_items_raw`, `payments_raw`, `returns_raw`, `attachments_raw`, `pdf_manifest_raw`
* **Audit Columns:** `row_status` (Default: `'FAIL'`), `ingestion_time`

#### **B. Curated Dataset (`curated_dataset`)**
*Golden records with strict schema (Types) and referential integrity.*
* **Tables:** `customers_curated`, `orders_curated`, `order_items_curated`, `payments_curated`, `returns_curated`, `attachments_curated`, `pdf_manifest_curated`
* **Audit Log:** `data_error_logs` (Centralized error tracking)

#### **C. Business Curated Dataset (`business_curated_dataset`)**
*Aggregated tables for reporting.*
* `customer_lifetime_value`
* `customer_segments`
* `monthly_sales`
* `product_performance`
* `product_returns`

---

## 🔄 Pipeline Orchestration (Airflow)

The system is driven by **4 Sequential DAGs**:

### 🟢 DAG 1 — Ingest & Link
**Goal:** Load raw data and physically validate PDFs.
1.  **Structured:** Load CSVs into BigQuery Raw tables.
    * Normalize: Set `row_status = 'FAIL'`, stamp `ingestion_time`.
    * Move processed CSVs to `processed/`.
2.  **Unstructured:** Scan PDF invoices & extract `order_id` from filename.
    * **Match:** Move to `archive/` → Insert into `attachments_raw`.
    * **Orphan:** Move to `error/` → Log in `data_error_logs`.

### 🟡 DAG 2 — Governance & Audit
**Goal:** Certify valid orders using the Gatekeeper logic.
* **Logic:** Updates `row_status` to `'PASS'` **only** if a valid attachment exists.
    ```sql
    UPDATE orders_raw
    SET row_status = 'PASS'
    WHERE order_id IN (SELECT order_id FROM attachments_raw);
    ```
* *Result:* All unverified orders remain as `'FAIL'`.

### 🔵 DAG 3 — Curation (Golden Records)
**Goal:** Produce strict, analytics-ready datasets.
* **Customers:** Fully curated.
* **Orders:** Only orders with `row_status = 'PASS'`.
* **Child Tables (Items, Payments, Returns):**
    * Filtered via: `WHERE order_id IN (SELECT order_id FROM orders_curated)`
* **Error Handling:** Failed orders are logged to `data_error_logs` to prevent reporting gaps.

### 🟣 DAG 4 — Business Analytics
**Goal:** Create business insight tables for dashboards.

| Table Name | Description |
| :--- | :--- |
| `customer_lifetime_value` | Total revenue per customer |
| `customer_segments` | High / Medium / Low value tiering |
| `monthly_sales` | Monthly revenue & order volume trends |
| `product_performance` | Revenue by product |
| `product_returns` | Return rates by product |

---

## 🔐 Security Model

We migrated from personal credentials to a **Service Account** architecture for production safety.

### **Authentication Strategy**
* ❌ **Original (Dev):** `gcloud auth application-default login` (Personal User)
* ✅ **Current (Prod):** IAM Service Account

### **Service Account Details**
* **Account:** `airflow-sa@archive-demo-project-484906.iam.gserviceaccount.com`
* **Configuration:** `GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/airflow-sa-key.json`

### **IAM Roles (Least Privilege)**
* `BigQuery Data Editor` (Read/Write Tables)
* `BigQuery Job User` (Run Queries)
* `Storage Object Admin` (Move/Rename Files)
* `Storage Bucket Viewer` (List Files)

---

## 🧪 Technologies Used

| Tool | Purpose |
| :--- | :--- |
| **Google Cloud Storage** | Data Lake (Landing, Archive) |
| **BigQuery** | Enterprise Data Warehouse |
| **Apache Airflow** | Workflow Orchestration & Scheduling |
| **Python** | Custom Pipeline Logic & GCS Operations |
| **SQL (Standard)** | Data Transformations & Business Logic |
| **Docker** | Local Airflow Runtime |

---

## 📊 Business Value

1.  **Regulatory Compliance:** Ensures 100% of reported revenue is backed by physical evidence.
2.  **Data Integrity:** Prevents "Ghost Orders" (orphans) from corrupting financial reports.
3.  **Analytics-Ready:** Provides clean, type-safe datasets for immediate BI consumption.
4.  **Actionable Insights:** Enables deep analysis of Customer Segments, Sales Trends, and Product Returns.

---

## 📁 Project Structure
```
airflow/
 ├── dags/
 │   ├── dag_1_ingestion.py        # Ingest CSVs & Process PDFs
 │   ├── dag_2_governance.py       # Gatekeeper Logic (Pass/Fail)
 │   ├── dag_3_curation.py         # Create Golden Records
 │   └── dag_4_business_analytics.py # KPI Tables
 └── sql/
     ├── curated_ddls.sql          # Table Definitions (Curated)
     └── business_ddls.sql         # Table Definitions (Business)

docker-compose.yml                 # Airflow Container Config
README.md                          # Project Documentation
```

## 🚀 Future Enhancements

1. **SCD Type 2:** Implement Slowly Changing Dimensions for Customer history.
2. **Data Quality:** Integrate Great Expectations for schema validation.
3. **BI Integration:** Connect datasets to Looker or Power BI.
4. **CDC:** Implement Change Data Capture for real-time ingestion.
5. **Alerting:** Configure Slack/Email alerts for Orphan PDF detection.
