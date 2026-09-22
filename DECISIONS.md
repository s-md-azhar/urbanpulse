# Architectural Decision Records (ADRs) — UrbanPulse Lakehouse

This document records the architectural, infrastructure, and engineering decisions made during the design and construction of the UrbanPulse data lakehouse platform. Every choice is evaluated against operational complexity, performance, resource footprint, and reproducibility.

---

## ADR 001: Polars & DuckDB over Apache Spark
- **Status:** Accepted
- **Context:** The pipeline processes hourly weather and air-quality measurements across 8 major Indian cities (~192 records per day per source, scaled to thousands during backfills). Distributed frameworks like Apache Spark or PySpark require JVM runtimes, cluster managers (YARN/K8s), significant memory overhead (>4GB per executor), and multi-node network serialization.
- **Decision:** Use **Polars** (written in Rust with Apache Arrow memory model) for Bronze/Silver data ingestion and transformations, and **DuckDB** for Gold dimensional modeling and SQL analytics.
- **Consequences:**
  - Fast execution (sub-second queries and transformations on standard single-node hardware).
  - Memory consumption stays comfortably within 200MB–500MB (fully runnable on 8–16GB developer laptops).
  - Zero JVM or Spark cluster maintenance overhead.
  - Scaling limitation: If data expands to hundreds of millions of rows across thousands of sensors, a transition to Dataproc/Spark or a distributed Arrow compute engine (e.g. Daft/Ray) would be required.

---

## ADR 002: Delta Lake (delta-rs) over Plain Parquet
- **Status:** Accepted
- **Context:** Many lightweight data engineering projects write flat Parquet files partitioned by date. However, Parquet alone lacks ACID transactions, atomic appends, schema enforcement, time-travel, and idempotent `MERGE/UPSERT` capabilities. Rerunning a pipeline job on plain Parquet risks partial writes, orphaned files, and duplicate rows.
- **Decision:** Adopt **Delta Lake** via the `deltalake` Python package (`delta-rs` native Rust bindings).
- **Consequences:**
  - True ACID transaction log (`_delta_log/`) on local storage or S3/MinIO.
  - Idempotent upserting: Merges on `(city, date, hour)` guarantee zero row duplication during reruns or backfills.
  - Schema validation preventing corrupted records from polluting the lake.
  - Can be queried natively by DuckDB, Polars, and Apache Spark.

---

## ADR 003: DuckDB with `dbt-duckdb` for the Gold Serving Layer
- **Status:** Accepted
- **Context:** The Silver layer consists of cleaned Delta tables. An analytical mart (Gold) requires dimensional modeling (`dim_city`, `fct_daily_city_metrics`), surrogate keys, aggregations, and rigorous data quality testing. Running a full PostgreSQL instance or cloud warehouse (Snowflake/BigQuery) requires live service management and credentials.
- **Decision:** Use **DuckDB** as the embedded analytical engine orchestrated by **`dbt-duckdb`**.
- **Consequences:**
  - Zero-setup file-based analytical database (`urbanpulse.duckdb`).
  - DuckDB natively queries Delta Lake tables via its delta extension and Arrow integration.
  - Full dbt feature set: lineage graphs, schema tests (`not_null`, `unique`, `accepted_range`), and custom SQL business assertions.
  - Complete portability without external network dependencies or API keys.

---

## ADR 004: Apache Airflow (LocalExecutor) & GitHub Actions Hybrid Orchestration
- **Status:** Accepted
- **Context:** We need a production-grade demonstration of data pipeline orchestration with dependency graphs, retries, and historical backfills, while also keeping the public dashboard continuously updated without paying for a 24/7 cloud VM to host an Airflow cluster.
- **Decision:**
  1. **Local Orchestration:** Provide a lean Apache Airflow `docker-compose.yml` (LocalExecutor, trimmed to Webserver, Scheduler, Postgres metadata DB) for local developer orchestration, DAG inspection, and backfills.
  2. **Automated CI/CD Refresh:** Use a scheduled **GitHub Actions workflow** (`cron: '0 3 * * *'`) that runs the pipeline directly via a Python runner script, executes dbt quality gates, retrains ML models, and commits snapshot JSON files to `dashboard/public/data/`.
- **Consequences:**
  - Demonstrates enterprise Airflow orchestration locally.
  - Keeps the public dashboard live and refreshed daily for zero cloud cost and zero server maintenance.

---

## ADR 005: HistGradientBoostingRegressor for Short-Horizon AQI Forecasting
- **Status:** Accepted
- **Context:** The system forecasts next-day AQI per city. Deep learning models (LSTMs, Transformers) or Facebook Prophet introduce excessive dependencies (PyTorch/TensorFlow, Stan C++ compilers), long training times, and opacity.
- **Decision:** Train per-city `HistGradientBoostingRegressor` (from `scikit-learn`) on engineered lag features (3-day lags of PM2.5, PM10, AQI, temperature, humidity, and wind speed) along with temporal signals (day of week, month).
- **Consequences:**
  - High accuracy on tabular time-series with non-linear weather interactions.
  - Native handling of missing values and rapid training (seconds per city).
  - Versioned model artifacts saved in `pipeline/ml/models/` alongside logged Mean Absolute Error (MAE) and feature importances.

---

## ADR 006: Next.js Static Export with Vercel Hosting
- **Status:** Accepted
- **Context:** The frontend dashboard needs to display city comparisons, interactive time-series charts, and pipeline observability metrics. Running a live Node.js/Python server backend creates cold starts, potential downtime, and hosting costs.
- **Decision:** Build a Next.js App Router application using TypeScript, Tailwind CSS, and Recharts, configured with `output: 'export'`. The dashboard consumes flat JSON snapshots from `dashboard/public/data/` generated during each pipeline run.
- **Consequences:**
  - Zero-maintenance static hosting with global CDN caching on Vercel.
  - Instant page load speeds and 100% uptime.
  - No environment variables or credentials required at build or runtime.
