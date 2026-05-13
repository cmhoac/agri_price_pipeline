# Project Overview: Agricultural Price Pipeline

This project is a robust, serverless data pipeline designed to automate the collection, processing, and storage of agricultural price data. It transitions from a local Docker/Airflow setup to a modern, cloud-native architecture using GitHub Actions and Neon PostgreSQL.

## 🏗️ Data Architecture: Medallion System
The project implements a **Medallion Architecture** to ensure data quality and traceability:

1.  **Bronze Layer (Raw)**:
    *   **Source**: Agricultural price portals (web scraping).
    *   **Format**: Raw HTML snippets or CSV files.
    *   **Storage**: `data/bronze/`
2.  **Silver Layer (Cleaned)**:
    *   **Process**: Data cleaning, standardization (units, names), and validation using **Pandera**.
    *   **Format**: Parquet (optimized for performance).
    *   **Storage**: `data/silver/`
3.  **Gold Layer (Curated)**:
    *   **Process**: Business logic application, aggregations, and deduplication.
    *   **Output**: Final analytical tables pushed to the **Neon PostgreSQL** cloud warehouse.
    *   **Storage**: `data/gold/` and Cloud DB.

---

## 🛠️ Tech Stack
*   **Language**: Python 3.11+
*   **Data Processing**:
    *   **Polars**: Primary engine for fast, multi-threaded data transformation.
    *   **Pandas**: Secondary engine for specific compatibility tasks.
*   **Data Validation**: **Pandera** for schema enforcement and quality checks.
*   **Web Scraping**: `requests` + `BeautifulSoup4`.
*   **Infrastructure & Orchestration**:
    *   **GitHub Actions**: Serverless orchestration (scheduled daily at 09:00 AM VN time).
    *   **Neon PostgreSQL**: Serverless cloud database for high-availability storage.
    *   **GitHub Secrets**: Secure management of DB credentials and SMTP keys.
*   **Reporting**: SMTP-based email notifications (Success/Failure alerts).

---

## 📂 Directory Structure
```text
agri_price_pipeline/
├── .github/workflows/      # Orchestration (Daily GitHub Action)
├── src/
│   ├── extract/            # Scrapers and ingestion logic
│   ├── transform/          # Medallion processing (Cleaner & Gold Maker)
│   └── utils/              # Database connectors, loggers, helpers
├── data/                   # Local storage for Bronze, Silver, Gold layers
├── bi/                     # BI configurations (e.g., PowerBI/Superset)
├── sql/                    # SQL scripts for DB schema management
├── dags/                   # Legacy Airflow DAGs (kept for reference)
├── requirements.txt        # Python dependency list
└── run_pipeline.py         # Main entry point for the daily execution
```

---

## 🔄 ETL Pipeline Logic

### 1. Extraction (Scraper)
*   Connects to agricultural portals.
*   Captures current market prices for various commodities (vegetables, fruits, livestock).
*   Saves time-stamped raw files into the Bronze layer.

### 2. Transformation (Cleaner)
*   **Cleaning**: Trims whitespace, standardizes date formats, and converts currency strings to numeric values.
*   **Standardization**: Maps varied commodity names to a unified naming convention.
*   **Validation**: Uses Pandera to ensure no nulls in critical columns and that data types are correct before moving to Silver.

### 3. Loading & Gold Creation (Gold Maker)
*   Reads Silver data.
*   Performs "Upsert" logic: Ensures that if a price for a specific date/commodity already exists in the cloud, it is updated; otherwise, it's inserted.
*   Pushes the final verified dataset to Neon PostgreSQL using `SQLAlchemy`.

---

## 🚀 System Design & Workflow
1.  **Trigger**: GitHub Actions cron job fires at 02:00 UTC daily.
2.  **Environment**: A fresh Ubuntu runner is provisioned.
3.  **Execution**: `run_pipeline.py` sequences the Extract -> Transform -> Load steps.
4.  **Database**: Neon DB receives the latest data, which can then be queried via BI tools.
5.  **Alerting**: The system sends a status email to the developer.
