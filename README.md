<img width="1400" height="820" alt="architecture_gcp_style" src="https://github.com/user-attachments/assets/3c5b5809-af63-4e64-a21d-52ce9888ae7d" /># crypto-datapipeline-gcp


# Crypto Exchange Data Pipeline (Airflow + GCP)

An end-to-end ELT data pipeline built with **Apache Airflow** that ingests live cryptocurrency market data from the **CoinGecko API** and loads it into **Google BigQuery** for analytics, using **Google Cloud Storage** as the data lake layer.

## Architecture

```
CoinGecko API → GCS (Raw Zone) → Transform (Pandas) → GCS (Transformed Zone) → BigQuery
```
![Uploading <svg width="1400" height="820" viewBox="0 0 1400 820" xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, Helvetica, Arial, sans-serif">
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
      <path d="M0,0 L0,6 L9,3 z" fill="#5f6368"/>
    </marker>
    <filter id="cardShadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="0" dy="1" stdDeviation="2.5" flood-color="#000" flood-opacity="0.18"/>
    </filter>
  </defs>

  <!-- Page background -->
  <rect width="1400" height="820" fill="#ffffff"/>

  <!-- Title -->
  <text x="700" y="58" text-anchor="middle" fill="#202124" font-size="34" font-family="Georgia, 'Times New Roman', serif">Crypto Data Pipeline on Google Cloud</text>

  <!-- Google Cloud boundary -->
  <rect x="300" y="95" width="1070" height="700" fill="#aecbfa" opacity="0.55"/>
  <text x="316" y="118" fill="#174ea6" font-size="13" font-weight="600">Google Cloud</text>
  <text x="1352" y="140" text-anchor="end" fill="#174ea6" font-size="12">Fetch data every 10 min &amp; store artifacts in Cloud Storage</text>

  <!-- ============ Data Sources panel (outside cloud) ============ -->
  <rect x="40" y="180" width="220" height="300" fill="#fce8b2"/>
  <text x="150" y="210" text-anchor="middle" fill="#3c4043" font-size="15" font-weight="600">Data Sources</text>
  <g filter="url(#cardShadow)">
    <rect x="65" y="280" width="170" height="90" rx="8" fill="#ffffff"/>
    <circle cx="95" cy="315" r="14" fill="#8ab000"/>
    <circle cx="95" cy="315" r="9" fill="#d7ee8e"/>
    <circle cx="99" cy="311" r="2.4" fill="#202124"/>
    <text x="118" y="313" fill="#202124" font-size="15" font-weight="600">CoinGecko</text>
    <text x="118" y="332" fill="#5f6368" font-size="12">Crypto API</text>
    <text x="118" y="352" fill="#80868b" font-size="10" letter-spacing="0.5">/COINS/MARKETS</text>
  </g>

  <!-- ============ Data Ingestion ============ -->
  <rect x="340" y="180" width="230" height="300" fill="#fce8b2"/>
  <text x="455" y="210" text-anchor="middle" fill="#3c4043" font-size="15" font-weight="600">Data Ingestion</text>
  <g filter="url(#cardShadow)">
    <rect x="360" y="260" width="190" height="100" rx="8" fill="#ffffff"/>
    <rect x="378" y="278" width="18" height="14" rx="2" fill="#4285f4"/>
    <rect x="378" y="295" width="18" height="6" rx="2" fill="#aecbfa"/>
    <text x="404" y="290" fill="#1a73e8" font-size="14" font-weight="600">Cloud Storage</text>
    <text x="404" y="308" fill="#202124" font-size="12.5">Data Lake — Raw Zone</text>
    <line x1="378" y1="320" x2="532" y2="320" stroke="#e8eaed" stroke-width="1.5"/>
    <text x="378" y="340" fill="#80868b" font-size="10" letter-spacing="0.5">RAW_DATA_STORAGE (.JSON)</text>
  </g>

  <!-- ============ Data Storage ============ -->
  <rect x="620" y="180" width="230" height="300" fill="#fce8b2"/>
  <text x="735" y="210" text-anchor="middle" fill="#3c4043" font-size="15" font-weight="600">Data Storage</text>
  <g filter="url(#cardShadow)">
    <rect x="640" y="260" width="190" height="100" rx="8" fill="#ffffff"/>
    <rect x="658" y="278" width="18" height="14" rx="2" fill="#4285f4"/>
    <rect x="658" y="295" width="18" height="6" rx="2" fill="#aecbfa"/>
    <text x="684" y="290" fill="#1a73e8" font-size="14" font-weight="600">Cloud Storage</text>
    <text x="684" y="308" fill="#202124" font-size="12.5">Data Lake — Curated</text>
    <line x1="658" y1="320" x2="812" y2="320" stroke="#e8eaed" stroke-width="1.5"/>
    <text x="658" y="340" fill="#80868b" font-size="10" letter-spacing="0.5">TRANSFORMED_DATA (.CSV)</text>
  </g>

  <!-- ============ Data Warehouse ============ -->
  <rect x="900" y="180" width="230" height="300" fill="#fce8b2"/>
  <text x="1015" y="210" text-anchor="middle" fill="#3c4043" font-size="15" font-weight="600">Data Warehouse</text>
  <g filter="url(#cardShadow)">
    <rect x="920" y="260" width="190" height="100" rx="8" fill="#ffffff"/>
    <circle cx="946" cy="290" r="11" fill="none" stroke="#4285f4" stroke-width="3"/>
    <line x1="954" y1="298" x2="962" y2="306" stroke="#4285f4" stroke-width="3" stroke-linecap="round"/>
    <text x="972" y="288" fill="#1a73e8" font-size="14" font-weight="600">BigQuery</text>
    <text x="972" y="306" fill="#202124" font-size="12.5">SQL Analytics</text>
    <line x1="938" y1="320" x2="1092" y2="320" stroke="#e8eaed" stroke-width="1.5"/>
    <text x="938" y="340" fill="#80868b" font-size="10" letter-spacing="0.5">CRYPTO_DB.TBL_CRYPTO</text>
  </g>

  <!-- ============ Insights ============ -->
  <rect x="1180" y="180" width="170" height="440" fill="#fce8b2"/>
  <text x="1265" y="205" text-anchor="middle" fill="#3c4043" font-size="14" font-weight="600">Applications &amp;</text>
  <text x="1265" y="223" text-anchor="middle" fill="#3c4043" font-size="14" font-weight="600">Insights</text>
  <g filter="url(#cardShadow)">
    <rect x="1195" y="260" width="140" height="95" rx="8" fill="#ffffff"/>
    <circle cx="1216" cy="285" r="8" fill="none" stroke="#4285f4" stroke-width="3"/>
    <text x="1230" y="290" fill="#1a73e8" font-size="13" font-weight="600">Looker</text>
    <text x="1210" y="312" fill="#202124" font-size="11.5">Embedded Analytics</text>
    <line x1="1210" y1="322" x2="1320" y2="322" stroke="#e8eaed" stroke-width="1.5"/>
    <text x="1210" y="340" fill="#80868b" font-size="9.5" letter-spacing="0.5">FIND_INSIGHTS</text>
  </g>
  <g filter="url(#cardShadow)">
    <rect x="1195" y="390" width="140" height="95" rx="8" fill="#ffffff"/>
    <rect x="1208" y="410" width="5" height="14" fill="#4285f4"/>
    <rect x="1216" y="405" width="5" height="19" fill="#669df6"/>
    <rect x="1224" y="414" width="5" height="10" fill="#aecbfa"/>
    <text x="1238" y="420" fill="#1a73e8" font-size="13" font-weight="600">Data Studio</text>
    <text x="1210" y="442" fill="#202124" font-size="11.5">Dashboards</text>
    <line x1="1210" y1="452" x2="1320" y2="452" stroke="#e8eaed" stroke-width="1.5"/>
    <text x="1210" y="470" fill="#80868b" font-size="9.5" letter-spacing="0.5">BUILD_DASHBOARD</text>
  </g>

  <!-- ============ Orchestration panel ============ -->
  <rect x="340" y="530" width="790" height="230" fill="#fce8b2"/>
  <text x="735" y="560" text-anchor="middle" fill="#3c4043" font-size="15" font-weight="600">Orchestrate and Manage Pipeline</text>

  <!-- Airflow mark (simple pinwheel) -->
  <g transform="translate(500,655)">
    <g>
      <path d="M0,0 L46,-10 L14,6 Z" fill="#e43921"/>
      <path d="M0,0 L10,46 L-6,14 Z" fill="#00ad46"/>
      <path d="M0,0 L-46,10 L-14,-6 Z" fill="#04d3f5"/>
      <path d="M0,0 L-10,-46 L6,-14 Z" fill="#017cee"/>
      <circle r="3.5" fill="#ffffff" stroke="#5f6368" stroke-width="1"/>
    </g>
    <text x="68" y="-8" fill="#3c4043" font-size="20">Apache</text>
    <text x="68" y="24" fill="#3c4043" font-size="30" font-weight="700">Airflow</text>
  </g>

  <g filter="url(#cardShadow)">
    <rect x="760" y="600" width="300" height="115" rx="8" fill="#ffffff"/>
    <rect x="782" y="620" width="16" height="16" rx="2" fill="#4285f4"/>
    <rect x="786" y="624" width="8" height="8" fill="#ffffff"/>
    <text x="808" y="634" fill="#1a73e8" font-size="15" font-weight="600">Cloud Composer / Airflow</text>
    <text x="782" y="658" fill="#202124" font-size="13">Pipeline Orchestration</text>
    <line x1="782" y1="672" x2="1038" y2="672" stroke="#e8eaed" stroke-width="1.5"/>
    <text x="782" y="694" fill="#80868b" font-size="11" letter-spacing="0.5">API → GCS → BIGQUERY PIPELINE · EVERY 10 MIN</text>
  </g>

  <!-- ============ Arrows ============ -->
  <!-- Source -> Ingestion -->
  <line x1="260" y1="315" x2="332" y2="315" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>
  <!-- Ingestion -> Storage -->
  <line x1="570" y1="315" x2="612" y2="315" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>
  <!-- Storage -> Warehouse -->
  <line x1="850" y1="315" x2="892" y2="315" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>
  <!-- Warehouse -> Insights -->
  <line x1="1130" y1="315" x2="1172" y2="315" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>

  <!-- Orchestration double arrows up to panels -->
  <line x1="455" y1="522" x2="455" y2="488" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>
  <line x1="465" y1="488" x2="465" y2="522" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>

  <line x1="730" y1="522" x2="730" y2="488" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>
  <line x1="740" y1="488" x2="740" y2="522" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>

  <line x1="1010" y1="522" x2="1010" y2="488" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>
  <line x1="1020" y1="488" x2="1020" y2="522" stroke="#5f6368" stroke-width="1.8" marker-end="url(#arr)"/>
</svg>
architecture_gcp_style.svg…]()



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
