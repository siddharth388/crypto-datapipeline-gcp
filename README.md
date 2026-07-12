# Crypto Exchange Data Pipeline (Airflow + GCP)

An end-to-end ELT data pipeline built with **Apache Airflow** that ingests live cryptocurrency market data from the **CoinGecko API** and loads it into **Google BigQuery** for analytics, using **Google Cloud Storage** as the data lake layer.

## Architecture

```
CoinGecko API → GCS (Raw Zone) → Transform (Pandas) → GCS (Transformed Zone) → BigQuery
```
<img width="1400" height="820" alt="architecture_gcp_style" src="https://github.com/user-attachments/assets/3c5b5809-af63-4e64-a21d-52ce9888ae7d" /># crypto-datapipeline-gcp




## Pipeline Overview

The DAG (`crypto_exchange_pipeline`) runs every **10 minutes** and orchestrates the following tasks:

1. **Create GCS Bucket** (`GCSCreateBucketOperator`) – Provisions a multi-regional GCS bucket to serve as the data lake (idempotent).
2. **Ingest Raw Data** (`HttpToGCSOperator`) – Fetches the top 10 cryptocurrencies by market cap (price, market cap, volume) from the CoinGecko `/coins/markets` endpoint and writes the raw JSON directly to the GCS raw zone, timestamped per run using Airflow's `{{ ts_nodash }}` template.
3. **Transform Data** (`PythonOperator` + `GCSHook`) – Downloads the raw JSON from GCS in-memory, selects relevant columns (id, symbol, name, current_price, market_cap, total_volume, last_updated), adds an ingestion timestamp, and converts it to CSV. The result is passed downstream via XCom — no local disk I/O.
4. **Load Transformed Data to GCS** – Uploads the transformed CSV to the GCS transformed zone.
5. **Create BigQuery Dataset & Table** – Idempotently provisions the target dataset (`crypto_db`) and table (`tbl_crypto`) with an explicit schema definition.
6. **Load to BigQuery** (`GCSToBigQueryOperator`) – Loads the transformed CSV from GCS into BigQuery with `WRITE_TRUNCATE` disposition and schema enforcement.

## Key Features

- **Zero local storage** – All data movement happens in-memory or GCS-to-GCS using hooks, making the pipeline container/worker agnostic
- **Run-level partitioned raw files** – Each execution writes a uniquely timestamped raw JSON, preserving ingestion history in the raw zone
- **Schema-enforced loading** – Explicit BigQuery schema with typed fields (STRING, FLOAT, TIMESTAMP) and nullability constraints
- **Idempotent infrastructure tasks** – Bucket, dataset, and table creation are safely re-runnable
- **Connection-based configuration** – API and GCP credentials managed via Airflow Connections (`coingecko_api`, `google_cloud_default`), keeping secrets out of code

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Data Source | CoinGecko REST API |
| Data Lake | Google Cloud Storage |
| Transformation | Pandas |
| Data Warehouse | Google BigQuery |
| Providers | `apache-airflow-providers-google` |
