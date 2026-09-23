"""
Unit and Integration Tests for UrbanPulse:
Tests idempotency of Bronze-to-Silver transformations and Delta Lake upsert semantics.
"""

import sys
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, date

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.silver.transform_to_silver import _idempotent_upsert_delta


@pytest.fixture
def temp_delta_dir():
    """Create a temporary directory for isolated Delta Lake table testing."""
    temp_dir = tempfile.mkdtemp(prefix="urbanpulse_test_delta_")
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_idempotent_upsert_zero_duplicates(temp_delta_dir):
    """
    Verify that executing an upsert twice on identical primary keys (city, date, hour)
    results in exactly the original row count with zero duplicates.
    """
    table_path = temp_delta_dir / "test_silver_weather"

    # Batch 1: 24 hourly records for Delhi on 2026-09-22
    records_run1 = []
    for h in range(24):
        records_run1.append({
            "city": "delhi",
            "date": date(2026, 9, 22),
            "hour": h,
            "datetime_utc": datetime(2026, 9, 22, h, 0, 0),
            "temperature_2m": 30.0 + h * 0.2,
            "relative_humidity_2m": 55.0,
            "precipitation": 0.0,
            "wind_speed_10m": 12.5,
            "weather_code": 1,
            "source_file": "raw/weather/delhi/2026-09-22.json",
            "silver_processed_at": "2026-09-22T10:00:00Z",
        })

    df1 = pl.DataFrame(records_run1)
    arrow1 = df1.to_arrow()

    # First upsert (initial write)
    _idempotent_upsert_delta(arrow1, table_path)

    result_df1 = pl.read_delta(str(table_path))
    assert len(result_df1) == 24, f"Expected 24 rows, got {len(result_df1)}"

    # Batch 2: Rerun on the exact same date and hours with slightly updated temperature (simulating a pipeline rerun/backfill)
    records_run2 = []
    for h in range(24):
        records_run2.append({
            "city": "delhi",
            "date": date(2026, 9, 22),
            "hour": h,
            "datetime_utc": datetime(2026, 9, 22, h, 0, 0),
            "temperature_2m": 31.0 + h * 0.2,  # Updated value
            "relative_humidity_2m": 55.0,
            "precipitation": 0.0,
            "wind_speed_10m": 12.5,
            "weather_code": 1,
            "source_file": "raw/weather/delhi/2026-09-22.json",
            "silver_processed_at": "2026-09-22T11:00:00Z",
        })

    df2 = pl.DataFrame(records_run2)
    arrow2 = df2.to_arrow()

    # Second upsert (the test of idempotency)
    _idempotent_upsert_delta(arrow2, table_path)

    result_df2 = pl.read_delta(str(table_path))

    # CRITICAL ASSERTION: Total rows must still be EXACTLY 24, not 48!
    assert len(result_df2) == 24, f"IDEMPOTENCY FAILED: Row count doubled to {len(result_df2)} instead of remaining 24."

    # Verify updated values took effect (ACID merge semantics)
    sample_hour_0 = result_df2.filter(pl.col("hour") == 0)
    assert sample_hour_0["temperature_2m"][0] == 31.0

    # Verify complete uniqueness of composite key (city, date, hour)
    key_counts = result_df2.group_by(["city", "date", "hour"]).len()
    assert (key_counts["len"] == 1).all(), "Duplicate composite keys detected in Silver Delta table!"


def test_delta_table_acid_versioning(temp_delta_dir):
    """
    Verify that Delta Lake increments table version on merge operations,
    providing full audit trail and time-travel capability.
    """
    table_path = temp_delta_dir / "test_acid_versioning"

    rec1 = pl.DataFrame([{
        "city": "mumbai",
        "date": date(2026, 9, 22),
        "hour": 12,
        "datetime_utc": datetime(2026, 9, 22, 12, 0, 0),
        "temperature_2m": 29.5,
        "relative_humidity_2m": 78.0,
        "precipitation": 0.5,
        "wind_speed_10m": 18.0,
        "weather_code": 2,
        "source_file": "raw/weather/mumbai/2026-09-22.json",
        "silver_processed_at": "2026-09-22T12:00:00Z",
    }]).to_arrow()

    _idempotent_upsert_delta(rec1, table_path)
    dt = DeltaTable(str(table_path))
    assert dt.version() == 0

    # Rerun update
    _idempotent_upsert_delta(rec1, table_path)
    dt.update_incremental()
    assert dt.version() == 1, "Delta table version was not incremented after merge."
