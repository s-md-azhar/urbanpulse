"""
Silver Layer Transformation:
Cleans, type-casts, validates, and performs idempotent upserts into Silver Delta tables.
Flags and quarantines malformed records to data/delta/quarantine.
Primary key: (city, date, hour).
"""

import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Add parent directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.config import (
    BRONZE_WEATHER_PATH,
    BRONZE_AQ_PATH,
    SILVER_WEATHER_PATH,
    SILVER_AQ_PATH,
    QUARANTINE_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("transform_to_silver")


def _quarantine_records(df: pl.DataFrame, reason: str, source_type: str) -> None:
    """Write quarantined records to quarantine Delta table."""
    if df.is_empty():
        return

    q_df = df.with_columns([
        pl.lit(reason).alias("quarantine_reason"),
        pl.lit(source_type).alias("source_type"),
        pl.lit(datetime.utcnow().isoformat() + "Z").alias("quarantined_at"),
    ])

    QUARANTINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(QUARANTINE_PATH),
        q_df.to_arrow(),
        mode="append",
        schema_mode="merge",
    )
    logger.warning("Quarantined %d malformed %s records. Reason: %s", len(df), source_type, reason)


def _idempotent_upsert_delta(
    arrow_table: pa.Table,
    target_path: Path,
    key_columns: Tuple[str, ...] = ("city", "date", "hour"),
) -> None:
    """
    Perform ACID idempotent upsert into a Delta table keyed on specified columns.
    If table does not exist, initializes it.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_str = str(target_path)

    if not Path(target_path / "_delta_log").exists():
        logger.info("Initializing Silver Delta table at %s", target_str)
        write_deltalake(
            target_str,
            arrow_table,
            mode="overwrite",
        )
        return

    dt = DeltaTable(target_str)
    predicate = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

    logger.info("Merging %d rows into Delta table at %s with predicate: %s", arrow_table.num_rows, target_str, predicate)
    (
        dt.merge(
            source=arrow_table,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def transform_weather() -> int:
    """
    Read Bronze weather, cast types, quarantine anomalies, and merge into Silver.
    """
    if not (BRONZE_WEATHER_PATH / "_delta_log").exists():
        logger.warning("Bronze weather table not found at %s", BRONZE_WEATHER_PATH)
        return 0

    bronze_dt = DeltaTable(str(BRONZE_WEATHER_PATH))
    df = pl.from_arrow(bronze_dt.to_pyarrow_table())

    if df.is_empty():
        return 0

    # Deduplicate bronze input by latest ingestion_timestamp
    df = df.sort("ingestion_timestamp").group_by(["city", "time"]).last()

    # Parse timestamps and generate primary keys
    # time format is typically "YYYY-MM-DDTHH:MM"
    df = df.with_columns([
        pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M").alias("datetime_utc"),
    ]).with_columns([
        pl.col("datetime_utc").dt.date().alias("date"),
        pl.col("datetime_utc").dt.hour().cast(pl.Int32).alias("hour"),
        pl.col("temperature_2m").cast(pl.Float64),
        pl.col("relative_humidity_2m").cast(pl.Float64),
        pl.col("precipitation").cast(pl.Float64),
        pl.col("wind_speed_10m").cast(pl.Float64),
        pl.col("weather_code").cast(pl.Int32),
        pl.lit(datetime.utcnow().isoformat() + "Z").alias("silver_processed_at"),
    ])

    # Quarantine filters:
    # Temperature: [-50, 65] °C
    # Humidity: [0, 100] %
    # Wind speed: [0, 180] km/h
    malformed_mask = (
        (pl.col("temperature_2m") < -50.0) | (pl.col("temperature_2m") > 65.0) |
        (pl.col("relative_humidity_2m") < 0.0) | (pl.col("relative_humidity_2m") > 100.0) |
        (pl.col("wind_speed_10m") < 0.0) | (pl.col("wind_speed_10m") > 180.0) |
        pl.col("date").is_null() | pl.col("city").is_null()
    )

    malformed_df = df.filter(malformed_mask)
    if not malformed_df.is_empty():
        _quarantine_records(malformed_df, "Weather measurement out of realistic atmospheric bounds", "weather")

    clean_df = df.filter(~malformed_mask)

    # Reorder columns
    clean_df = clean_df.select([
        "city",
        "date",
        "hour",
        "datetime_utc",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code",
        "source_file",
        "silver_processed_at",
    ])

    _idempotent_upsert_delta(clean_df.to_arrow(), SILVER_WEATHER_PATH)
    logger.info("Silver weather transformation completed. Clean rows processed: %d", len(clean_df))
    return len(clean_df)


def transform_air_quality() -> int:
    """
    Read Bronze air quality, cast types, quarantine anomalies, and merge into Silver.
    """
    if not (BRONZE_AQ_PATH / "_delta_log").exists():
        logger.warning("Bronze air quality table not found at %s", BRONZE_AQ_PATH)
        return 0

    bronze_dt = DeltaTable(str(BRONZE_AQ_PATH))
    df = pl.from_arrow(bronze_dt.to_pyarrow_table())

    if df.is_empty():
        return 0

    df = df.sort("ingestion_timestamp").group_by(["city", "time"]).last()

    df = df.with_columns([
        pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M").alias("datetime_utc"),
    ]).with_columns([
        pl.col("datetime_utc").dt.date().alias("date"),
        pl.col("datetime_utc").dt.hour().cast(pl.Int32).alias("hour"),
        pl.col("pm10").cast(pl.Float64),
        pl.col("pm2_5").cast(pl.Float64),
        pl.col("carbon_monoxide").cast(pl.Float64),
        pl.col("nitrogen_dioxide").cast(pl.Float64),
        pl.col("sulphur_dioxide").cast(pl.Float64),
        pl.col("ozone").cast(pl.Float64),
        pl.col("european_aqi").cast(pl.Float64),
        pl.col("us_aqi").cast(pl.Float64),
        pl.lit(datetime.utcnow().isoformat() + "Z").alias("silver_processed_at"),
    ])

    # Quarantine filters:
    # PM2.5: [0, 1500] ug/m3
    # PM10: [0, 2000] ug/m3
    # US AQI: [0, 1000]
    malformed_mask = (
        (pl.col("pm2_5") < 0.0) | (pl.col("pm2_5") > 1500.0) |
        (pl.col("pm10") < 0.0) | (pl.col("pm10") > 2000.0) |
        (pl.col("us_aqi") < 0.0) | (pl.col("us_aqi") > 1000.0) |
        pl.col("date").is_null() | pl.col("city").is_null()
    )

    malformed_df = df.filter(malformed_mask)
    if not malformed_df.is_empty():
        _quarantine_records(malformed_df, "Air quality pollutant concentration out of physical bounds", "air_quality")

    clean_df = df.filter(~malformed_mask)

    clean_df = clean_df.select([
        "city",
        "date",
        "hour",
        "datetime_utc",
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "european_aqi",
        "us_aqi",
        "source_file",
        "silver_processed_at",
    ])

    _idempotent_upsert_delta(clean_df.to_arrow(), SILVER_AQ_PATH)
    logger.info("Silver air quality transformation completed. Clean rows processed: %d", len(clean_df))
    return len(clean_df)


def run_silver_transformations():
    """Execute Silver transformations for weather and air quality."""
    w = transform_weather()
    aq = transform_air_quality()
    logger.info("Silver transformation complete. Weather: %d, Air Quality: %d", w, aq)
    return {"weather_clean_rows": w, "air_quality_clean_rows": aq}


if __name__ == "__main__":
    run_silver_transformations()
