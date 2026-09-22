# UrbanPulse — Multi-City Weather & Air Quality Intelligence Lakehouse

[![Daily Lakehouse Refresh](https://github.com/s-md-azhar/urbanpulse/actions/workflows/daily_refresh.yml/badge.svg)](https://github.com/s-md-azhar/urbanpulse/actions/workflows/daily_refresh.yml)
[![Vercel Ready](https://img.shields.io/badge/Vercel-Deployment%20Ready-black?logo=vercel)](https://vercel.com)
[![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion%20Delta%20Lake-blue)](https://delta.io)
[![Data Quality](https://img.shields.io/badge/Quality%20Gate-dbt--duckdb-orange)](https://getdbt.com)
[![Machine Learning](https://img.shields.io/badge/Forecasting-HistGradientBoosting-purple)](https://scikit-learn.org)

A daily-refreshing, production-grade data lakehouse that ingests live weather and air-quality data for 8 major Indian cities, processes it through a Medallion Delta Lake architecture, enforces strict dbt quality gates, forecasts next-day AQI per city with versioned ML models, and delivers insights to a Next.js static dashboard.

---

## 🌆 Business Framing & Value Proposition

Urban logistics fleets (e.g. quick commerce, delivery dispatch), retail footfall planning, and municipal public-health advisories heavily depend on short-horizon atmospheric forecasts. 
- **Last-Mile Logistics:** Sudden spikes in AQI or localized rainfall trigger courier gear requirements, alter delivery ETA algorithms, and increase driver fatigue.
- **Retail Footfall:** High particulate matter (PM2.5 > 150 µg/m³) directly correlates with decreased discretionary retail footfall and surging online grocery orders.
- **Public Health Authorities:** Preemptive alerts based on predictive next-day atmospheric inversions enable proactive warnings for vulnerable populations.

UrbanPulse simulates the exact ingestion-to-insight lakehouse pipeline an enterprise data platform team would deploy, implementing the critical production aspects that standard tutorials omit: **idempotent reruns, ACID delta commits, automated schema quarantine, hard dbt data quality gates, historical backfills, and continuous ML inference.**

---

## 🏗️ Architecture & Medallion Data Flow

```mermaid
flowchart TD
    subgraph Sources ["External APIs (Free & Keyless)"]
        W[Open-Meteo Weather API]
        AQ[Open-Meteo Air Quality API]
    end

    subgraph Ingestion ["Ingestion & Landing"]
        FETCH[Python httpx + tenacity Backoff]
        RAW[(data/raw/ - Immutable JSON Landing)]
    end

    subgraph Medallion ["Medallion Lakehouse Engine"]
        BRONZE[(Bronze Delta Lake: Append-Only)]
        QUARANTINE[(Quarantine Delta: Malformed Records)]
        SILVER[(Silver Delta Lake: Idempotent Upsert)]
        GATE{dbt Data Quality Hard Gate}
        GOLD[(Gold DuckDB Marts: dim_city & fct_daily)]
    end

    subgraph ML ["Predictive Intelligence"]
        TRAIN[HistGradientBoosting AQI Forecaster]
        PRED[(Predictions Delta Table)]
        MODELS[Versioned Artifacts: pipeline/ml/models/]
    end

    subgraph Serving ["Serving & Presentation"]
        EXPORT[Flat JSON Snapshots: dashboard/public/data/]
        DASH[Next.js App Router + Recharts Dashboard on Vercel]
    end

    W --> FETCH
    AQ --> FETCH
    FETCH --> RAW
    RAW -->|Polars Parsing + Hash Verification| BRONZE
    BRONZE -->|Type Casting & Validation| SILVER
    BRONZE -.->|Out of Bounds Data| QUARANTINE
    SILVER --> GATE
    GATE -->|Tests Pass| GOLD
    GOLD -->|Lagged Weather & AQI Features| TRAIN
    TRAIN --> PRED
    TRAIN --> MODELS
    GOLD --> EXPORT
    PRED --> EXPORT
    EXPORT --> DASH
```

### Medallion Tier Breakdown
1. **Raw Landing Zone (`data/raw/{source}/{city}/{date}.json`):**
   - Immutable raw HTTP payloads fetched via `httpx` with exponential backoff (`tenacity`).
   - Stored in a directory hierarchy compatible with local filesystem or cloud object storage (AWS S3 / MinIO).
2. **Bronze Layer (`data/delta/bronze_*`):**
   - Parsed with Polars from raw JSON payloads.
   - Stamped with ingestion timestamps, payload sha256 checksums, and source lineage.
   - Appended atomically to Delta Lake tables using Rust native bindings (`deltalake`).
3. **Silver Layer (`data/delta/silver_*`):**
   - Strictly typed schemas and UTC timestamp standardization.
   - Out-of-bound or corrupted records are automatically routed to `data/delta/quarantine`.
   - **Idempotent Merge:** Delta table upsert keyed on `(city, date, hour)` guarantees zero duplicate rows upon reruns or backfills.
4. **Gold Layer (`data/gold/urbanpulse.duckdb`):**
   - Modeled via `dbt-duckdb`:
     - `dim_city`: Static city geography, coordinates, and classification.
     - `fct_daily_city_metrics`: Daily rollups, rolling 7-day baselines, and multi-day lags.
5. **Machine Learning Layer (`pipeline/ml/`):**
   - Per-city `HistGradientBoostingRegressor` models trained on 3-day lagged AQI, temperature, relative humidity, and wind speed.
   - Logs Mean Absolute Error (MAE) and persists versioned models (`.joblib`) and forecasts (`data/delta/predictions`).

---

## 🎯 Target Cities

| City | State | Coordinates | Classification |
| :--- | :--- | :--- | :--- |
| **Delhi** | National Capital Territory | 28.6139° N, 77.2090° E | Tier-1 Megacity |
| **Mumbai** | Maharashtra | 19.0760° N, 72.8777° E | Tier-1 Megacity |
| **Bengaluru** | Karnataka | 12.9716° N, 77.5946° E | Tier-1 Tech Hub |
| **Kolkata** | West Bengal | 22.5726° N, 88.3639° E | Tier-1 Metro |
| **Chennai** | Tamil Nadu | 13.0827° N, 80.2707° E | Tier-1 Coastal Hub |
| **Hyderabad** | Telangana | 17.3850° N, 78.4867° E | Tier-1 Tech Hub |
| **Pune** | Maharashtra | 18.5204° N, 73.8567° E | Tier-2 Emerging Metro |
| **Ahmedabad** | Gujarat | 23.0225° N, 72.5714° E | Tier-2 Industrial Hub |

---

## 🚀 Quickstart Guide (Local Environment)

The pipeline requires **zero external credentials** and runs out-of-the-box on single-node developer laptops (8–16 GB RAM).

### Option A: Direct Python Execution (No Docker Required)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/s-md-azhar/urbanpulse.git
   cd urbanpulse
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv .venv
   # Windows:
   .\.venv\Scripts\activate
   # Linux/macOS:
   source .venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Execute End-to-End Pipeline with 14-day Backfill:**
   ```bash
   python pipeline/run_pipeline.py --backfill-days 14
   ```
   This autonomously ingests 14 days of historical data, builds Bronze and Silver Delta tables, executes dbt quality gates, updates the DuckDB Gold mart, trains ML models, and exports snapshots into `dashboard/public/data/`.

5. **Run Automated Idempotency Tests:**
   ```bash
   pytest pipeline/tests/test_idempotency.py -v
   ```

6. **Launch Frontend Dashboard Locally:**
   ```bash
   cd dashboard
   npm install
   npm run dev
   ```
   Open `http://localhost:3000` to interact with the dashboard.

---

## 🐳 Docker & Airflow Orchestration Verification

UrbanPulse includes an enterprise Apache Airflow orchestration stack via `docker-compose.yml` (LocalExecutor, trimmed to Webserver, Scheduler, Postgres, and mapped volumes).

### Docker Verification Steps
If Docker Desktop is installed on your machine, follow these steps to verify Airflow orchestration:

1. **Copy environment variables:**
   ```bash
   cp .env.example .env
   ```

2. **Start the Airflow stack:**
   ```bash
   docker compose up -d
   ```

3. **Verify running containers:**
   ```bash
   docker compose ps
   ```
   *What success looks like:*
   ```
   NAME                           IMAGE                               COMMAND                  STATUS
   urbanpulse-postgres-1          postgres:15-alpine                  "docker-entrypoint.s…"   Up (healthy)
   urbanpulse-airflow-init-1      apache/airflow:2.8.3-python3.11     "/usr/bin/dumb-init …"   Exited (0)
   urbanpulse-airflow-webserver-1 apache/airflow:2.8.3-python3.11     "/usr/bin/dumb-init …"   Up (healthy)
   urbanpulse-airflow-scheduler-1 apache/airflow:2.8.3-python3.11     "/usr/bin/dumb-init …"   Up (healthy)
   ```

4. **Access the Airflow UI:**
   Navigate to `http://localhost:8080` (Username: `airflow`, Password: `airflow`).
   - Trigger the DAG: `urbanpulse_daily_pipeline`.
   - The DAG executes: `[ingest_weather, ingest_air_quality] -> bronze_delta_ingest -> silver_idempotent_transform -> data_quality_hard_gate -> gold_dimensional_marts -> ml_aqi_forecasting -> export_dashboard_snapshots`.

5. **Stop the stack:**
   ```bash
   docker compose down
   ```

---

## 🌐 Vercel Deployment

The frontend (`/dashboard`) is a static Next.js App Router application configured with `output: 'export'` that reads committed JSON snapshots.

> [!TIP]
> **Vercel Zero-Config Deployment:**
> Import this repository into Vercel with root directory set to `/dashboard` — no other configuration, environment variables, or build overrides needed!

---

## 🧪 Data Quality & Quality Gates

Data quality is enforced using **dbt-duckdb** as a hard blocking gate:
- **Null Checks & Uniqueness:** Verified on primary keys `(city, date, hour)`.
- **Accepted Ranges:** Temperature `[-50, 65]°C`, Humidity `[0, 100]%`, Wind `[0, 180] km/h`.
- **Custom AQI Atmospheric Sanity (`test_aqi_sanity.sql`):** Ensures all AQI values fall strictly within realistic physical bounds `[0, 500]` and asserts `max_us_aqi >= avg_us_aqi`.
- **Hard Gate Semantic:** If any test fails, the pipeline raises `RuntimeError` and terminates immediately before Gold analytics or ML models can consume the tainted batch.

---

## 📈 Machine Learning AQI Forecaster

- **Algorithm:** `HistGradientBoostingRegressor` (from `scikit-learn`).
- **Target Variable:** Next-day average US AQI.
- **Engineered Features:**
  - 3-day AQI memory (`lag_1d_aqi`, `lag_2d_aqi`, `lag_3d_aqi`).
  - Atmospheric predictors (`avg_temperature_c`, `avg_humidity_pct`, `avg_wind_speed_kmh`).
  - Fine/Coarse particulate matter (`avg_pm2_5`, `avg_pm10`).
  - Temporal cyclical markers (`day_of_week`, `month`).
- **Telemetry & Artifacts:**
  - Versioned model weights saved to `pipeline/ml/models/{city}_aqi_model_v1.joblib`.
  - Continuous validation against a naive persistence baseline (`yesterday's AQI`), logging Mean Absolute Error (MAE) for every city.

---

## ⚖️ Limitations & What I'd Change at 10x Scale

| Dimension | Current Implementation (UrbanPulse) | Bottleneck at 10x Scale | 10x Enterprise Architecture |
| :--- | :--- | :--- | :--- |
| **Storage** | Local disk structured as `data/raw/` and `data/delta/` | Local I/O contention, single-point-of-failure | Amazon S3 / Google Cloud Storage with MinIO for local dev |
| **Compute Engine** | Polars (single node Arrow) + DuckDB | Memory exhaustion if records expand to millions of IoT sensors | Apache Spark on Dataproc / EMR or distributed Arrow via Daft / Ray |
| **Orchestration** | Airflow LocalExecutor + GitHub Actions | Sequential task execution and host resource limits | Airflow CeleryExecutor / KubernetesExecutor on Cloud Composer / MWAA |
| **Warehouse** | Single-file DuckDB (`urbanpulse.duckdb`) | Concurrent write locking during multi-tenant analytical queries | Snowflake, Google BigQuery, or Trino querying Iceberg/Delta lakehouse tables |
| **Feature Store** | File-based lagged queries | Offline/online skew, duplicate feature transformations | Feast or Hopsworks Feature Store with automated drift monitoring (Evidently AI) |
| **Serving** | Flat JSON snapshots committed to Git | Git repository bloat from high-frequency updates | Serverless edge API (FastAPI on AWS Lambda or Cloudflare Workers with Redis caching) |

---

## 📄 License

MIT License. Designed and engineered for production-grade data platform portfolio reference.
