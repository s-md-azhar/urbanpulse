"""
Ingestion module for Open-Meteo Weather API.
Handles HTTP requests, retries with exponential backoff using tenacity,
and writes raw immutable JSON payloads to data/raw/weather/{city}/{date}.json.
"""

import sys
import json
import logging
from datetime import datetime, date, timedelta
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

    def backfill(self, days: int = 14) -> None:
        """Backfill weather data for the past N days."""
        today = datetime.now().date()
        logger.info("Starting weather backfill for past %d days...", days)
        for i in range(days, 0, -1):
            past_date = today - timedelta(days=i)
            logger.info("Backfilling weather for date: %s", past_date)
            self.fetch_all_cities(past_date)
        logger.info("Weather backfill completed.")

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
