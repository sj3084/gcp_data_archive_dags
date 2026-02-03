# GCP Data Archival & Retrieval Platform

**Compliance-First Data Engineering Architecture on Google Cloud**

This project implements an **enterprise-grade Archival, Compliance & Analytics Data Platform** on **Google Cloud Platform (GCP)**.  
It ingests structured business data (CSV) and unstructured physical evidence (PDF invoices), dynamically links them, and enforces strict governance rules before promoting data to analytics layers.

Unlike traditional ETL pipelines, this system follows a **Gatekeeper Pattern**:  
> **Data is only promoted if physical proof exists.**

---

## 📌 Core Business Rule
> **“No Order is valid without physical proof (Invoice PDF).”**

Only orders with verified invoice PDFs are promoted to the **Golden Record (Operational Curated Layer)** and then to the **Business Analytics Layer**.

This guarantees:
* ✅ Regulatory compliance
* ✅ Zero orphan records in analytics
* ✅ Full auditability
* ✅ Late-arrival recovery

---

## 🧩 Key Features
* ✅ **Hybrid Ingestion (CSV + PDF)**
* 🛡️ **Gatekeeper Architecture** (hard compliance boundary)
* 🗂️ **Zoned Data Lake** (Landing → Processed → Archive → Error)
* 🧾 **Physical proof enforcement**
* 🔁 **Late-arrival recovery** (Housekeeping DAG)
* 📊 **BI-ready datasets**
* 🔐 **IAM Service Account security**
* 🧮 **Full audit trail** (`data_error_logs`)
* ⚙️ **Apache Airflow orchestration** (5 DAGs)

---

## 🏗️ System Architecture

### 1. Storage Layer – Google Cloud Storage (Data Lake)

| Zone | Path | Description |
| :--- | :--- | :--- |
| **Landing** | `landing/structured/` | Incoming CSV files |
| | `landing/unstructured/` | Incoming invoice PDFs |
| **Processed** | `processed/` | Archived structured files |
| **Archive** | `archive/pdfs/` | Validated invoice PDFs |
| **Error** | `error/pdfs/` | Orphan or invalid PDFs |

**Benefits:**
* Clear lifecycle management
* Audit-safe (no deletes, only moves)
* Physical isolation of bad data

---

### 2. Data Warehouse – BigQuery

#### A. Raw Dataset (`raw_dataset`)
*Purpose: Staging layer (loose schema).*

**Tables:** `customers_raw`, `orders_raw`, `order_items_raw`,  
`payments_raw`, `returns_raw`, `attachments_raw`, `pdf_manifest_raw`

**Metadata Columns:** * `row_status` (default = `FAIL`)
* `ingestion_time`

#### B. Curated Dataset (`curated_dataset`)
*Purpose: Golden Record layer (strict schema & referential integrity).*

**Tables:** `customers_curated`, `orders_curated`, `order_items_curated`,  
`payments_curated`, `returns_curated`, `attachments_curated`, `pdf_manifest_curated`

**Audit Table:** `data_error_logs`

* **Tracks:** Compliance failures, Orphan PDFs, Invalid PDFs, Recovery events
* **Includes:** `resolved_flag`, `resolved_at`

#### C. Business Curated Dataset (`business_curated_dataset`)
*Purpose: Analytics & BI.*

**Tables:**
* `customer_lifetime_value`
* `customer_segments`
* `monthly_sales`
* `product_performance`
* `product_returns`

**Guarantee:** Analytics only use compliant data.

---

## 🔄 Pipeline Orchestration (Apache Airflow – 5 DAGs)

The platform uses a **Sequential Dependency Architecture**.

### DAG 1 — Ingest & Link
**Goal:** Ingest raw data and physically validate PDFs.

1.  Load CSVs → Raw tables
2.  Normalize metadata (`row_status`, `ingestion_time`)
3.  Process PDFs in parallel:
    * **Valid** → `archive/pdfs/` + `attachments_raw`
    * **Orphan** → `error/pdfs/` + `data_error_logs`
    * **Invalid** → `error/pdfs/` + `data_error_logs`

### DAG 2 — Governance & Audit
**Goal:** Certification layer (Gatekeeper).

* **SQL-only**
* Sets `row_status = PASS` only if attachment exists
* Orders without PDFs remain `FAIL`

*Creates a hard compliance boundary.*

### DAG 3 — Operational Curation (Golden Records)
**Goal:** Create trusted datasets.

* Filters only `row_status = PASS` orders
* Cascades filtering to: `order_items`, `payments`, `returns`, `attachments`

*Guarantee: Zero orphan records.*

### DAG 4 — Business Analytics Curation
**Goal:** BI-ready datasets.

* **Creates:** Customer Lifetime Value, Customer Segments, Monthly Sales, Product Performance, Product Returns

### DAG 5 — Housekeeping & Remediation
**Goal:** Recover late-arriving invoices.

*Runs independently of ingestion.*

**Logic:**
1.  Scans `landing/unstructured/` for new PDFs
2.  Matches against unresolved compliance failures
3.  **If found:**
    * Moves PDF → `archive/pdfs/`
    * Inserts into `attachments_raw`
    * Updates `data_error_logs` (`resolved_flag = true`, `resolved_at = timestamp`)

**Enables:**
* Late invoice recovery
* Error lifecycle tracking
* Regulatory audit history

*Governance (DAG 2) is rerun intentionally after remediation.*

---

## 🔐 Security & Authentication

Uses a dedicated **IAM Service Account** (no user credentials).

* **Service Account:** `airflow-sa@archive-demo-project-484906.iam.gserviceaccount.com`
* **Authentication:** `GOOGLE_APPLICATION_CREDENTIALS`
* **IAM Roles:**
    * BigQuery Data Editor
    * BigQuery Job User
    * Storage Object Admin

**Benefits:** Least privilege, Auditable, Portable, Production-ready.

---

## 🧪 Technologies Used

| Tool | Purpose |
| :--- | :--- |
| **Google Cloud Storage** | Data Lake |
| **BigQuery** | Enterprise Data Warehouse |
| **Apache Airflow** | Orchestration |
| **Python** | PDF parsing, concurrency, recovery logic |
| **SQL (Standard)** | Governance & transformations |
| **Docker** | Local Airflow runtime |

---

## 📊 Business Impact

1.  **Regulatory compliance** – every order has physical proof
2.  **Golden records** – zero orphan analytics
3.  **Trusted BI** – accurate KPIs
4.  **Operational excellence** – retryable, auditable, recoverable
5.  **Error lifecycle management** – unresolved → resolved tracking

*This platform transforms raw, chaotic data into a governed enterprise data asset.*

---

## 📁 Project Structure

```
airflow/
├── dags/
│   ├── dag_1_ingestion.py
│   ├── dag_2_governance.py
│   ├── dag_3_curation.py
│   ├── dag_4_business_analytics.py
│   └── dag_5_housekeeping.py
└── sql/
    ├── curated_ddls.sql
    └── business_ddls.sql

docker-compose.yml
README.md
```

## 🚀 Future Enhancements

1. **SCD Type 2:** Implement Slowly Changing Dimensions for Customer history.
2. **Data Quality:** Integrate Great Expectations for schema validation.
3. **BI Integration:** Connect datasets to Looker or Power BI.
4. **CDC:** Implement Change Data Capture for real-time ingestion.
5. **Alerting:** Configure Slack/Email alerts for Orphan PDF detection.
