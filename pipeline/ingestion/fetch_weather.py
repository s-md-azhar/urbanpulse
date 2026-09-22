"""
Ingestion module for Open-Meteo Weather API.
Handles HTTP requests, retries with exponential backoff using tenacity,
and writes raw immutable JSON payloads to data/raw/weather/{city}/{date}.json.
"""

import sys
import json
import logging
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Add parent directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.config import (
    CITIES,
    RAW_DATA_DIR,
    OPEN_METEO_WEATHER_FORECAST_URL,
    OPEN_METEO_WEATHER_ARCHIVE_URL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("fetch_weather")


class WeatherIngestor:
    """Ingests hourly weather data from Open-Meteo for specified cities and dates."""

    def __init__(self, raw_dir: Optional[Path] = None):
        self.raw_dir = raw_dir or (RAW_DATA_DIR / "weather")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.Client(timeout=30.0)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=16),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        reraise=True,
    )
    def _fetch_from_endpoint(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch data from endpoint with exponential backoff."""
        response = self.client.get(url, params=params)
        if response.status_code == 429:
            logger.warning("Rate limit hit (429), backing off...")
        response.raise_for_status()
        return response.json()

    def fetch_city_date(self, city_key: str, target_date: date) -> Path:
        """
        Fetch hourly weather for a specific city and date.
        Writes to immutable raw storage. Returns the path of the saved raw file.
        """
        city_info = CITIES[city_key]
        city_slug = city_key.lower()
        city_raw_dir = self.raw_dir / city_slug
        city_raw_dir.mkdir(parents=True, exist_ok=True)

        date_str = target_date.strftime("%Y-%m-%d")
        primary_file = city_raw_dir / f"{date_str}.json"

        # Check if already present to honor immutability
        if primary_file.exists():
            logger.info("Raw weather file for %s on %s already exists (immutable).", city_slug, date_str)
            return primary_file

        params = {
            "latitude": city_info["latitude"],
            "longitude": city_info["longitude"],
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
            "timezone": city_info["timezone"],
            "start_date": date_str,
            "end_date": date_str,
        }

        # Choose forecast or historical archive based on date age
        days_ago = (datetime.now().date() - target_date).days
        url = OPEN_METEO_WEATHER_ARCHIVE_URL if days_ago >= 5 else OPEN_METEO_WEATHER_FORECAST_URL

        logger.info("Fetching weather for %s on %s from %s", city_slug, date_str, "archive" if days_ago >= 5 else "forecast")
        try:
            payload = self._fetch_from_endpoint(url, params)
        except Exception as e:
            # Fallback to alternative endpoint if one fails
            alt_url = OPEN_METEO_WEATHER_FORECAST_URL if url == OPEN_METEO_WEATHER_ARCHIVE_URL else OPEN_METEO_WEATHER_ARCHIVE_URL
            logger.warning("Primary endpoint failed (%s). Retrying with alternative endpoint %s...", e, alt_url)
            payload = self._fetch_from_endpoint(alt_url, params)

        # Attach metadata
        payload["_metadata"] = {
            "city": city_slug,
            "city_name": city_info["name"],
            "target_date": date_str,
            "source": "open-meteo-weather",
            "ingested_at": datetime.utcnow().isoformat() + "Z",
        }

        # Atomic write
        temp_file = city_raw_dir / f".{date_str}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        temp_file.replace(primary_file)

        logger.info("Saved raw weather data to %s", primary_file)
        return primary_file

    def fetch_all_cities(self, target_date: date) -> Dict[str, Path]:
        """Fetch weather data for all configured cities on a target date."""
        results = {}
        for city_key in CITIES:
            results[city_key] = self.fetch_city_date(city_key, target_date)
        return results

    def backfill(self, days: int = 60) -> None:
        """
        Backfill historical weather data for the past N days across all cities.
        Uses efficient date-range requests and slices into immutable daily JSON files.
        """
        today = datetime.now().date()
        start_date = today - timedelta(days=days)
        end_date = today - timedelta(days=1)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        logger.info("Starting historical weather backfill for past %d days (%s to %s)...", days, start_str, end_str)

        for city_key, city_info in CITIES.items():
            city_slug = city_key.lower()
            city_raw_dir = self.raw_dir / city_slug
            city_raw_dir.mkdir(parents=True, exist_ok=True)

            # Check if any dates in range are missing
            missing_dates = []
            cur = start_date
            while cur <= end_date:
                d_str = cur.strftime("%Y-%m-%d")
                if not (city_raw_dir / f"{d_str}.json").exists():
                    missing_dates.append(d_str)
                cur += timedelta(days=1)

            if not missing_dates:
                logger.info("All %d historical weather files already exist for %s.", days, city_slug)
                continue

            logger.info("Fetching %d missing weather days for %s from archive...", len(missing_dates), city_slug)
            params = {
                "latitude": city_info["latitude"],
                "longitude": city_info["longitude"],
                "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
                "timezone": city_info["timezone"],
                "start_date": start_str,
                "end_date": end_str,
            }

            try:
                payload = self._fetch_from_endpoint(OPEN_METEO_WEATHER_ARCHIVE_URL, params)
            except Exception as e:
                logger.warning("Archive endpoint error (%s). Trying forecast endpoint...", e)
                payload = self._fetch_from_endpoint(OPEN_METEO_WEATHER_FORECAST_URL, params)

            hourly = payload.get("hourly", {})
            times = hourly.get("time", [])

            # Group indices by date
            from collections import defaultdict
            date_indices = defaultdict(list)
            for idx, t in enumerate(times):
                d = t.split("T")[0]
                date_indices[d].append(idx)

            for d_str, indices in date_indices.items():
                day_file = city_raw_dir / f"{d_str}.json"
                if day_file.exists():
                    continue

                day_payload = {
                    "latitude": payload.get("latitude"),
                    "longitude": payload.get("longitude"),
                    "timezone": payload.get("timezone"),
                    "hourly": {
                        "time": [times[i] for i in indices],
                        "temperature_2m": [hourly.get("temperature_2m", [])[i] for i in indices if i < len(hourly.get("temperature_2m", []))],
                        "relative_humidity_2m": [hourly.get("relative_humidity_2m", [])[i] for i in indices if i < len(hourly.get("relative_humidity_2m", []))],
                        "precipitation": [hourly.get("precipitation", [])[i] for i in indices if i < len(hourly.get("precipitation", []))],
                        "wind_speed_10m": [hourly.get("wind_speed_10m", [])[i] for i in indices if i < len(hourly.get("wind_speed_10m", []))],
                        "weather_code": [hourly.get("weather_code", [])[i] for i in indices if i < len(hourly.get("weather_code", []))],
                    },
                    "_metadata": {
                        "city": city_slug,
                        "city_name": city_info["name"],
                        "target_date": d_str,
                        "source": "open-meteo-weather",
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                }

                temp_f = city_raw_dir / f".{d_str}.tmp"
                with open(temp_f, "w", encoding="utf-8") as f:
                    json.dump(day_payload, f, indent=2)
                temp_f.replace(day_file)

            logger.info("Backfill complete for %s weather.", city_slug)

        logger.info("Historical weather backfill finished successfully.")

    def close(self):
        self.client.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch weather data from Open-Meteo")
    parser.add_argument("--date", type=str, help="Target date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--backfill", type=int, help="Backfill N days of historical data")
    args = parser.parse_args()

    ingestor = WeatherIngestor()
    try:
        if args.backfill:
            ingestor.backfill(args.backfill)
        else:
            run_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else datetime.now().date()
            ingestor.fetch_all_cities(run_date)
    finally:
        ingestor.close()
