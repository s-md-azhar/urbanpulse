"""
UrbanPulse Production Airflow DAG:
Orchestrates multi-city weather and air-quality ingestion, Medallion Delta Lake architecture,
dbt data quality hard gates, Gold dimensional modeling, and ML AQI forecasting.
Fully parameterized by execution_date for idempotent backfills.
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure pipeline root is in python path
PIPELINE_ROOT = os.getenv("PYTHONPATH", "/opt/airflow/pipeline")
if PIPELINE_ROOT not in sys.path:
    sys.path.insert(0, PIPELINE_ROOT)


def pipeline_failure_callback(context):
    """Structured failure observability callback for automated incident logging."""
    import json
    import logging
    from datetime import datetime
    from pipeline.config import DATA_DIR

    logger = logging.getLogger("urbanpulse.observability")
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    dag_id = ti.dag_id if ti else "urbanpulse_daily_pipeline"
    run_id = str(context.get("run_id", "unknown"))
    execution_date = str(context.get("logical_date") or context.get("execution_date"))
    exception = str(context.get("exception", "No exception details provided"))

    alert_payload = {
        "event": "PIPELINE_TASK_FAILURE",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "execution_date": execution_date,
        "try_number": getattr(ti, "try_number", 1),
        "error": exception[:500],
        "severity": "CRITICAL" if task_id == "data_quality_hard_gate" else "ERROR",
    }
    logger.error("PIPELINE_ALERT: %s", json.dumps(alert_payload))

    # Persist structured failure log to disk for observability auditing
    alert_log = DATA_DIR / "lakehouse_alerts.log"
    try:
        alert_log.parent.mkdir(parents=True, exist_ok=True)
        with open(alert_log, "a", encoding="utf-8") as f:
            f.write(json.dumps(alert_payload) + "\n")
    except Exception as log_err:
        logger.warning("Failed writing to lakehouse_alerts.log: %s", log_err)


default_args = {
    "owner": "urbanpulse-data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": pipeline_failure_callback,
}


def ingest_weather_op(**context):
    from pipeline.ingestion.fetch_weather import WeatherIngestor
    exec_date = context["logical_date"].date()
    ingestor = WeatherIngestor()
    try:
        results = ingestor.fetch_all_cities(exec_date)
        return {city: str(p) for city, p in results.items()}
    finally:
        ingestor.close()


def ingest_air_quality_op(**context):
    from pipeline.ingestion.fetch_air_quality import AirQualityIngestor
    exec_date = context["logical_date"].date()
    ingestor = AirQualityIngestor()
    try:
        results = ingestor.fetch_all_cities(exec_date)
        return {city: str(p) for city, p in results.items()}
    finally:
        ingestor.close()


def bronze_ingestion_op(**context):
    from pipeline.bronze.ingest_to_bronze import run_bronze_ingestion
    return run_bronze_ingestion()


def silver_transformation_op(**context):
    from pipeline.silver.transform_to_silver import run_silver_transformations
    return run_silver_transformations()


def dbt_quality_gate_op(**context):
    """
    Hard Data Quality Gate:
    Runs dbt tests against Silver staging views. If any test fails, raises exception
    and immediately halts downstream Gold mart generation.
    """
    import os
    import subprocess
    from pipeline.config import DUCKDB_PATH, DELTA_DATA_DIR
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    dbt_dir = Path(__file__).resolve().parent.parent.parent / "pipeline" / "dbt_project"
    if not dbt_dir.exists():
        dbt_dir = Path("/opt/airflow/pipeline/dbt_project")

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(DUCKDB_PATH.resolve())
    env["DELTA_DATA_PATH"] = str(DELTA_DATA_DIR.resolve())

    # First refresh staging views to match latest silver delta tables
    run_res = subprocess.run(
        ["dbt", "run", "--select", "staging", "--profiles-dir", str(dbt_dir)],
        cwd=str(dbt_dir),
        capture_output=True,
        text=True,
        env=env,
    )
    if run_res.returncode != 0:
        raise RuntimeError(f"STAGING MATERIALIZATION FAILED:\n{run_res.stdout}\n{run_res.stderr}")

    # Next execute hard quality gate tests
    test_res = subprocess.run(
        ["dbt", "test", "--select", "staging", "--profiles-dir", str(dbt_dir)],
        cwd=str(dbt_dir),
        capture_output=True,
        text=True,
        env=env,
    )
    if test_res.returncode != 0:
        raise RuntimeError(f"HARD QUALITY GATE FAILED:\n{test_res.stdout}\n{test_res.stderr}")
    return "Data quality gate passed successfully."


def dbt_gold_marts_op(**context):
    import os
    import subprocess
    from pipeline.config import DUCKDB_PATH, DELTA_DATA_DIR
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    dbt_dir = Path(__file__).resolve().parent.parent.parent / "pipeline" / "dbt_project"
    if not dbt_dir.exists():
        dbt_dir = Path("/opt/airflow/pipeline/dbt_project")

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(DUCKDB_PATH.resolve())
    env["DELTA_DATA_PATH"] = str(DELTA_DATA_DIR.resolve())

    res = subprocess.run(
        ["dbt", "run", "--select", "marts", "--profiles-dir", str(dbt_dir)],
        cwd=str(dbt_dir),
        capture_output=True,
        text=True,
        env=env,
    )
    if res.returncode != 0:
        raise RuntimeError(f"dbt run marts failed:\n{res.stdout}\n{res.stderr}")
    return "Gold marts created successfully."


def ml_forecasting_op(**context):
    from pipeline.ml.train import run_ml_pipeline
    return run_ml_pipeline()


def export_dashboard_snapshots_op(**context):
    from pipeline.run_pipeline import export_dashboard_snapshots
    return export_dashboard_snapshots()


with DAG(
    dag_id="urbanpulse_daily_pipeline",
    default_args=default_args,
    description="Daily ingestion, Medallion processing, quality gating, and ML forecasting for UrbanPulse",
    schedule="0 3 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    on_failure_callback=pipeline_failure_callback,
    tags=["lakehouse", "medallion", "weather", "air-quality", "ml"],
) as dag:

    t1_weather = PythonOperator(
        task_id="ingest_weather",
        python_callable=ingest_weather_op,
    )

    t1_aq = PythonOperator(
        task_id="ingest_air_quality",
        python_callable=ingest_air_quality_op,
    )

    t2_bronze = PythonOperator(
        task_id="bronze_delta_ingest",
        python_callable=bronze_ingestion_op,
    )

    t3_silver = PythonOperator(
        task_id="silver_idempotent_transform",
        python_callable=silver_transformation_op,
    )

    t4_quality_gate = PythonOperator(
        task_id="data_quality_hard_gate",
        python_callable=dbt_quality_gate_op,
    )

    t5_gold_marts = PythonOperator(
        task_id="gold_dimensional_marts",
        python_callable=dbt_gold_marts_op,
    )

    t6_ml_forecasting = PythonOperator(
        task_id="ml_aqi_forecasting",
        python_callable=ml_forecasting_op,
    )

    t7_export_snapshots = PythonOperator(
        task_id="export_dashboard_snapshots",
        python_callable=export_dashboard_snapshots_op,
    )

    # Dependency Graph:
    # [Ingest Weather, Ingest AQ] -> Bronze -> Silver -> Quality Gate -> Gold Marts -> ML Forecast -> Export
    [t1_weather, t1_aq] >> t2_bronze >> t3_silver >> t4_quality_gate >> t5_gold_marts >> t6_ml_forecasting >> t7_export_snapshots
