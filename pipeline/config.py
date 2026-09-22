"""
UrbanPulse Configuration Module
Defines target cities, coordinates, API endpoints, and path resolutions.
"""

from pathlib import Path
from typing import Dict, Any

# Root paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
DELTA_DATA_DIR = DATA_DIR / "delta"
GOLD_DATA_DIR = DATA_DIR / "gold"
DUCKDB_PATH = GOLD_DATA_DIR / "urbanpulse.duckdb"

# Delta table paths
BRONZE_WEATHER_PATH = DELTA_DATA_DIR / "bronze_weather"
BRONZE_AQ_PATH = DELTA_DATA_DIR / "bronze_air_quality"
SILVER_WEATHER_PATH = DELTA_DATA_DIR / "silver_weather"
SILVER_AQ_PATH = DELTA_DATA_DIR / "silver_air_quality"
PREDICTIONS_PATH = DELTA_DATA_DIR / "predictions"
QUARANTINE_PATH = DELTA_DATA_DIR / "quarantine"

# Target Cities (Hardcoded lat/longs per spec)
CITIES: Dict[str, Dict[str, Any]] = {
    "delhi": {
        "name": "Delhi",
        "state": "National Capital Territory",
        "latitude": 28.6139,
        "longitude": 77.2090,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-1 Megacity",
    },
    "mumbai": {
        "name": "Mumbai",
        "state": "Maharashtra",
        "latitude": 19.0760,
        "longitude": 72.8777,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-1 Megacity",
    },
    "bengaluru": {
        "name": "Bengaluru",
        "state": "Karnataka",
        "latitude": 12.9716,
        "longitude": 77.5946,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-1 Tech Hub",
    },
    "kolkata": {
        "name": "Kolkata",
        "state": "West Bengal",
        "latitude": 22.5726,
        "longitude": 88.3639,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-1 Metro",
    },
    "chennai": {
        "name": "Chennai",
        "state": "Tamil Nadu",
        "latitude": 13.0827,
        "longitude": 80.2707,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-1 Coastal Hub",
    },
    "hyderabad": {
        "name": "Hyderabad",
        "state": "Telangana",
        "latitude": 17.3850,
        "longitude": 78.4867,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-1 Tech Hub",
    },
    "pune": {
        "name": "Pune",
        "state": "Maharashtra",
        "latitude": 18.5204,
        "longitude": 73.8567,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-2 Emerging Metro",
    },
    "ahmedabad": {
        "name": "Ahmedabad",
        "state": "Gujarat",
        "latitude": 23.0225,
        "longitude": 72.5714,
        "timezone": "Asia/Kolkata",
        "population_tier": "Tier-2 Industrial Hub",
    },
}

# Open-Meteo Endpoints
OPEN_METEO_WEATHER_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_WEATHER_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
