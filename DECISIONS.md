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

---

## ADR 007: Expanded 60-Day Historical Backfill, Native NaN Warm-Up, & Honest ML Baseline Findings
- **Status:** Accepted
- **Context:** The initial prototype backfilled only 14 days of data. When engineering lag features ($t-1, t-2, t-3$) and leading target values ($t+1$), a 14-day window yielded only 10–11 labeled observations per city.
  1. **Sample Starvation:** Evaluating a train/test split on 11 rows meant an 80/20 split left only 2 rows in the test set, creating extreme metric variance.
  2. **Risk of Random Shuffling Leakage:** Random train/test splits on time-series data leak adjacent-day atmospheric conditions (since weather is autoregressive), producing artificially near-zero MAEs.
  3. **In-Sample Overfitting:** Evaluating training error on small sample sizes yields illusory perfection (MAE < 0.1) that fails in production.
  4. **Warm-Up Row Math:** On 60 historical days with 3-day lags, the first 3 days per city do not have prior lags. Discarding them would drop 3 valuable days per city (leaving only 56 labeled samples).
- **Decision:**
  1. Increase the historical backfill window to **60 days** using Open-Meteo's historical archive endpoint (`https://archive-api.open-meteo.com/v1/archive` and historical air quality range queries).
  2. Enforce a **strict chronological train/test split (80% train on earlier 47 days, 20% test on latest 12 days)** for every city model.
  3. **Native NaN Warm-Up Handling:** Rather than discarding warm-up rows or using arbitrary imputation, leverage `HistGradientBoostingRegressor`'s native missing-value (`NaN`) binning for days 1–3 lags. This preserves 59 usable labeled observations (only the final day without a target is dropped).
  4. Evaluate and log genuine **out-of-sample Test MAE** alongside a **naive persistence baseline** ($AQI_{t+1} \approx AQI_t$) evaluated on the exact same held-out test window.
  5. Train the final production inference artifact on all 59 labeled days to forecast tomorrow ($t+1$).
  6. **Deterministic Seed Pinning:** Explicitly set `RANDOM_STATE = 42` across all regressor instances and record hyperparameter configurations directly into versioned model metadata.
  7. **Small-N Hyperparameter Optimization:** `scikit-learn`'s default `min_samples_leaf=20` is tuned for datasets with $N \ge 1{,}000$. On $N_{\text{train}}=47$, a leaf size of 20 allows only 1–2 splits total, causing models to predict near the global sample mean and underperform persistence across 7 of 8 cities. We tuned `min_samples_leaf=4`, `learning_rate=0.08`, and `max_iter=100`, allowing trees to capture real non-linear atmospheric interactions.
- **Consequences & Empirical Findings:**
  - **Sample Grounding:** Provides 59 labeled daily observations per city (47 training days, 12 out-of-sample test days).
  - **Deterministic Reproducibility:** Fixed `random_state=42` guarantees exact metric reproducibility across every pipeline run, local test, and GitHub Actions cron cycle.
  - **Honest Performance Disclosure:** **4 of 8 city models underperform the naive persistence baseline**, likely due to the small per-city training set (47 days) and low atmospheric volatility in coastal/peninsular cities (Bengaluru -8.1%, Mumbai -32.0%, Chennai -33.1%, Hyderabad -52.0%). In contrast, models demonstrate substantial genuine gains (+13% to +32%) specifically in higher-volatility, weather-transition cities (Kolkata +31.5%, Ahmedabad +31.5%, Delhi +17.7%, Pune +13.4%).
  - **Hyperparameter Grid Findings:** Even after an extensive grid search testing `min_samples_leaf` $\in \{2, 3, 4, 6, 20\}$, shallower depths (`max_depth=3`), and $L_2$ regularization, the low-volatility cities (Mumbai, Chennai, Hyderabad) persistently favor the 1-line persistence heuristic. On short-horizon series where $\Delta_{\text{day-over-day}} < 8$, the estimation variance of an 11-feature GBDT on $N=47$ exceeds the modest bias of persistence.
  - **Production Architecture Insight:** A production lakehouse should implement a hybrid routing rule that defaults to persistence for low-volatility regions while dispatching ML models for high-variance regions.

