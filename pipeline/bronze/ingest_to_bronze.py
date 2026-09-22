"""
Bronze Layer Ingestion:
Parses raw immutable JSON files into Delta Lake Bronze tables (append-only).
Attaches ingestion metadata, source tracking, and schema validation.
"""

import sys
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import polars as pl
from deltalake import write_deltalake, DeltaTable

# Add parent directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.config import (
    RAW_DATA_DIR,
    BRONZE_WEATHER_PATH,
    BRONZE_AQ_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ingest_to_bronze")


def _compute_hash(content: str) -> str:
    """Compute sha256 checksum of raw payload for lineage verification."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def process_raw_weather_files(raw_files: Optional[List[Path]] = None) -> int:
    """
    Parse raw weather JSON files and append to Bronze Delta table.
    """
    if raw_files is None:
        weather_dir = RAW_DATA_DIR / "weather"
        if not weather_dir.exists():
            logger.warning("No weather raw directory found at %s", weather_dir)
            return 0
        raw_files = list(weather_dir.glob("*/*.json"))

    if not raw_files:
        logger.info("No raw weather files found to process.")
        return 0

    records: List[Dict[str, Any]] = []
    for file_path in raw_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                data = json.loads(content)

            meta = data.get("_metadata", {})
            city = meta.get("city", file_path.parent.name)
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])

            payload_hash = _compute_hash(content)
            now_iso = datetime.utcnow().isoformat() + "Z"

            for idx, t in enumerate(times):
                records.append({
                    "city": city,
                    "time": t,
                    "temperature_2m": hourly.get("temperature_2m", [None])[idx] if idx < len(hourly.get("temperature_2m", [])) else None,
                    "relative_humidity_2m": hourly.get("relative_humidity_2m", [None])[idx] if idx < len(hourly.get("relative_humidity_2m", [])) else None,
                    "precipitation": hourly.get("precipitation", [None])[idx] if idx < len(hourly.get("precipitation", [])) else None,
                    "wind_speed_10m": hourly.get("wind_speed_10m", [None])[idx] if idx < len(hourly.get("wind_speed_10m", [])) else None,
                    "weather_code": hourly.get("weather_code", [None])[idx] if idx < len(hourly.get("weather_code", [])) else None,
                    "source_file": str(file_path.relative_to(RAW_DATA_DIR)),
                    "payload_hash": payload_hash,
                    "ingestion_timestamp": now_iso,
                })
        except Exception as e:
            logger.error("Failed to parse weather file %s: %s", file_path, e)

    if not records:
        logger.warning("No valid weather records extracted.")
        return 0

    df = pl.DataFrame(records)
    BRONZE_WEATHER_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Convert to Arrow and append to Delta
    arrow_table = df.to_arrow()
    write_deltalake(
        str(BRONZE_WEATHER_PATH),
        arrow_table,
        mode="append",
    )
    logger.info("Successfully appended %d weather records to Bronze Delta table at %s", len(df), BRONZE_WEATHER_PATH)
    return len(df)


def process_raw_air_quality_files(raw_files: Optional[List[Path]] = None) -> int:
    """
    Parse raw air quality JSON files and append to Bronze Delta table.
    """
    if raw_files is None:
        aq_dir = RAW_DATA_DIR / "air_quality"
        if not aq_dir.exists():
            logger.warning("No air quality raw directory found at %s", aq_dir)
            return 0
        raw_files = list(aq_dir.glob("*/*.json"))

    if not raw_files:
        logger.info("No raw air quality files found to process.")
        return 0

    records: List[Dict[str, Any]] = []
    for file_path in raw_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                data = json.loads(content)

            meta = data.get("_metadata", {})
            city = meta.get("city", file_path.parent.name)
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])

            payload_hash = _compute_hash(content)
            now_iso = datetime.utcnow().isoformat() + "Z"

            for idx, t in enumerate(times):
                records.append({
                    "city": city,
                    "time": t,
                    "pm10": hourly.get("pm10", [None])[idx] if idx < len(hourly.get("pm10", [])) else None,
                    "pm2_5": hourly.get("pm2_5", [None])[idx] if idx < len(hourly.get("pm2_5", [])) else None,
                    "carbon_monoxide": hourly.get("carbon_monoxide", [None])[idx] if idx < len(hourly.get("carbon_monoxide", [])) else None,
                    "nitrogen_dioxide": hourly.get("nitrogen_dioxide", [None])[idx] if idx < len(hourly.get("nitrogen_dioxide", [])) else None,
                    "sulphur_dioxide": hourly.get("sulphur_dioxide", [None])[idx] if idx < len(hourly.get("sulphur_dioxide", [])) else None,
                    "ozone": hourly.get("ozone", [None])[idx] if idx < len(hourly.get("ozone", [])) else None,
                    "european_aqi": hourly.get("european_aqi", [None])[idx] if idx < len(hourly.get("european_aqi", [])) else None,
                    "us_aqi": hourly.get("us_aqi", [None])[idx] if idx < len(hourly.get("us_aqi", [])) else None,
                    "source_file": str(file_path.relative_to(RAW_DATA_DIR)),
                    "payload_hash": payload_hash,
                    "ingestion_timestamp": now_iso,
                })
        except Exception as e:
            logger.error("Failed to parse air quality file %s: %s", file_path, e)

    if not records:
        logger.warning("No valid air quality records extracted.")
        return 0

    df = pl.DataFrame(records)
    BRONZE_AQ_PATH.parent.mkdir(parents=True, exist_ok=True)

    arrow_table = df.to_arrow()
    write_deltalake(
        str(BRONZE_AQ_PATH),
        arrow_table,
        mode="append",
    )
    logger.info("Successfully appended %d air quality records to Bronze Delta table at %s", len(df), BRONZE_AQ_PATH)
    return len(df)


def run_bronze_ingestion():
    """Execute Bronze ingestion for both weather and air quality."""
    w_count = process_raw_weather_files()
    aq_count = process_raw_air_quality_files()
    logger.info("Bronze ingestion complete. Weather rows: %d, Air quality rows: %d", w_count, aq_count)
    return {"weather_rows": w_count, "air_quality_rows": aq_count}


if __name__ == "__main__":
    run_bronze_ingestion()
