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

## ADR 007: Expanded 60-Day Historical Backfill, Native NaN Warm-Up, 3-Way Chronological Split (Train/Val/Test), & Plain-Disclosure Baseline Findings
- **Status:** Accepted
- **Context:** 
  1. **Sample Starvation in Early Iterations:** The initial prototype backfilled only 14 days of data. When engineering 3-day lag features ($t-1, t-2, t-3$) and leading target values ($t+1$), a 14-day window yielded only 10–11 labeled observations per city.
  2. **Risk of Random Shuffling Leakage:** Random train/test splits on time-series data leak adjacent-day atmospheric conditions (since weather is autoregressive), producing artificially near-zero MAEs. A chronological split is mandatory.
  3. **Diagnosing Hyperparameter Selection Leakage:** In an intermediate tuning pass, candidate hyperparameters (`min_samples_leaf=4, learning_rate=0.08`) were selected by evaluating every candidate configuration directly against the held-out 12-day test set. Even though the feature definitions were strictly lagged ($t$ or earlier), using test set performance to pick winning hyperparameters violates out-of-sample isolation. It introduces **selection leakage**, yielding overly optimistic test metrics that do not represent true out-of-sample generalization.
  4. **Warm-Up Row Math:** On 60 historical days with 3-day lags, the first 3 days per city do not have prior lags. Discarding them would drop 3 valuable days per city (leaving only 56 labeled samples).
- **Decision:**
  1. Increase the historical backfill window to **60 days** using Open-Meteo's historical archive endpoint (`https://archive-api.open-meteo.com/v1/archive` and historical air quality range queries).
  2. **Native NaN Warm-Up Handling:** Rather than discarding warm-up rows or using arbitrary imputation, leverage `HistGradientBoostingRegressor`'s native missing-value (`NaN`) binning for days 1–3 lags. This preserves all 59 usable labeled observations (only the final day without a target is dropped).
  3. **Strict 3-Way Chronological Partitioning (39 Train / 8 Val / 12 Test):**
     - **Training Window (Days 1–39, 39 days):** Earliest historical sequence used to fit candidate hyperparameter configurations.
     - **Validation Window (Days 40–47, 8 days):** Chronological window immediately prior to the test set, used **exclusively** for hyperparameter grid search and model selection. The test set is completely locked and unseen during this process.
     - **Test Window (Days 48–59, 12 days):** The final held-out test set, reserved strictly for a **single out-of-sample evaluation pass**.
  4. **Validation Grid Search Across 6 Architectures:** Evaluated 6 candidate configurations across all 8 cities strictly on the validation window:
     - `depth_3_leaf_3` (`min_samples_leaf=3, learning_rate=0.05, max_depth=3`) -> **Val MAE: 9.33 (Winner)**
     - `leaf_3_lr_06` (`min_samples_leaf=3, learning_rate=0.06`) -> Val MAE: 9.57
     - `leaf_4_lr_08` (`min_samples_leaf=4, learning_rate=0.08`) -> Val MAE: 9.58
     - `leaf_2_lr_05` (`min_samples_leaf=2, learning_rate=0.05`) -> Val MAE: 9.71
     - `leaf_5_lr_05` (`min_samples_leaf=5, learning_rate=0.05`) -> Val MAE: 10.35
     - `default_leaf_20` (`min_samples_leaf=20, learning_rate=0.1`) -> Val MAE: 15.75
     *Finding:* Restricting tree depth (`max_depth=3`) acts as a powerful structural regularizer, preventing trees from overfitting to noise in the 39-day training window while learning shallow, robust non-linear interaction rules.
  5. **Train + Validation Refit Strategy:** Once `depth_3_leaf_3` was selected via validation, the model was retrained on **Train + Validation combined (Days 1–47, 47 days)** prior to out-of-sample test evaluation.
     *Rationale:* Discarding the 8 days of validation data immediately preceding the test window would artificially handicap the model with stale distribution shifts. Refitting on Train+Val incorporates the most recent data leading up to day 48 while ensuring that hyperparameter choices were completely unpolluted by test set information.
  6. **Single Test Evaluation & Naive Baseline Comparison:** Evaluated the winning model **exactly once** on the untouched 12-day test set alongside a naive persistence baseline ($AQI_{t+1} \approx AQI_t$) computed on the same 12-day window.
  7. **Production Inference Fit:** Fit the final production artifact on all 59 labeled days to generate tomorrow's live forecast.
  8. **Deterministic Seed Pinning:** Explicitly set `RANDOM_STATE = 42` across all models and persist full split metadata, validation scores, and test metrics to `pipeline/ml/models/{city}_metadata.json`.
- **Consequences & Empirical Findings:**
  - **Zero Selection Leakage:** Clean separation between training, hyperparameter selection, and test evaluation. Test metrics are now genuine out-of-sample estimates.
  - **Honest Performance Disclosure:**
    - **5 of 8 cities beat the naive persistence baseline:** Kolkata (+29.8% gain, MAE 16.03 vs 22.83), Ahmedabad (+31.8% gain, MAE 7.20 vs 10.57), Bengaluru (+14.1% gain, MAE 6.22 vs 7.24), Delhi (+9.2% gain, MAE 17.61 vs 19.38), and Pune (+1.6% gain, MAE 8.08 vs 8.21).
    - **3 of 8 cities underperform persistence:** Mumbai (-32.3%, MAE 10.69 vs 8.08), Chennai (-30.1%, MAE 12.49 vs 9.60), and Hyderabad (-47.1%, MAE 12.10 vs 8.22).
  - **Why Low-Volatility Cities Favor Persistence:** In peninsular and coastal cities during stable meteorological periods, day-over-day AQI drift is minimal ($\Delta < 8$ points). In such low-variance regimes, an 11-feature GBDT trained on small sample sizes suffers from estimation variance that exceeds the modest bias of a 1-line persistence rule. In contrast, in high-volatility continental/industrial hubs (Delhi, Kolkata, Ahmedabad) where weather fronts cause rapid inversions and pollutant trapping, non-linear atmospheric interactions provide substantial predictive signal (+10% to +32% improvement).
  - **Metric Shift Explanation:** Compared to the preliminary test-leaked run (average MAE 11.08), the leak-free model yields an average test MAE of 11.30 (+4.0% overall improvement over the 11.77 baseline). The slight shift is expected: properly holding out validation data during hyperparameter selection eliminates optimistic test-peeking bias.

