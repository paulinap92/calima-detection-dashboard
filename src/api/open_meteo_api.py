"""
Air Quality API Client (Open-Meteo).

This module provides a lightweight client for fetching air-quality data
(PM10, PM2.5, dust, aerosol optical depth) from the Open-Meteo Air Quality API.

It is designed for:
- Canary Islands locations
- Hourly data processing
- Calima (Saharan dust) monitoring
- Integration with dashboards, schedulers, and persistence layers

All timestamps are handled as naive UTC datetime objects.

API source:
https://air-quality-api.open-meteo.com/v1/air-quality
"""

import requests
from dataclasses import dataclass
from datetime import datetime
from typing import Any
import logging

logger = logging.getLogger(__name__)

# ----------------------------------------------------------
# Location definitions (lat, lon)
# ----------------------------------------------------------

LOCATIONS: dict[str, tuple[float, float]] = {
    "santa_cruz": (28.4636, -16.2518),
    "puerto_de_la_cruz": (28.4140, -16.5449),
    "adeje": (28.1227, -16.7260),
}

FORECAST_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# ----------------------------------------------------------
# Data Models
# ----------------------------------------------------------


@dataclass
class AirQualityData:
    """
    Container for hourly air-quality time series.

    Attributes:
        time: List of hourly timestamps (naive UTC datetimes).
        pm10: PM10 concentration values (µg/m³).
        pm25: PM2.5 concentration values (µg/m³).
        dust: Dust concentration values.
        aod: Aerosol Optical Depth values.
    """

    time: list[datetime]
    pm10: list[float | None]
    pm25: list[float | None]
    dust: list[float | None]
    aod: list[float | None]


@dataclass
class CurrentPoint:
    """
    Representation of a single air-quality datapoint.

    Attributes:
        location: Location identifier.
        time: Timestamp of the datapoint (naive UTC).
        pm10: PM10 concentration.
        pm25: PM2.5 concentration.
        dust: Dust concentration.
        aod: Aerosol Optical Depth.
        is_calima: Whether the datapoint meets Calima conditions.
    """

    location: str
    time: datetime
    pm10: float | None
    pm25: float | None
    dust: float | None
    aod: float | None
    is_calima: bool


# ----------------------------------------------------------
# Helper functions
# ----------------------------------------------------------


def _to_datetime_list(times: list[str]) -> list[datetime]:
    """
    Convert ISO-formatted timestamp strings to naive UTC datetimes.

    Args:
        times: List of timestamp strings returned by the API.

    Returns:
        List of naive datetime objects in UTC.
    """
    return [
        datetime.fromisoformat(t).replace(tzinfo=None)
        for t in times
    ]


def _nearest_index(times: list[datetime], target: datetime) -> int:
    """
    Find the index of the timestamp closest to a target datetime.

    Args:
        times: List of datetime objects.
        target: Target datetime.

    Returns:
        Index of the closest timestamp.
    """
    diffs = [(abs((t - target).total_seconds())) for t in times]
    return diffs.index(min(diffs))


# ----------------------------------------------------------
# Historical fetch
# ----------------------------------------------------------


def fetch_history_days(location: str, days: int) -> AirQualityData:
    """
    Fetch historical hourly air-quality data.

    Args:
        location: Location key defined in LOCATIONS.
        days: Number of past days to fetch (maximum 90).

    Returns:
        AirQualityData containing historical hourly measurements.

    Raises:
        ValueError: If the location is unknown or days > 90.
        requests.HTTPError: If the API request fails.
    """
    if location not in LOCATIONS:
        raise ValueError(f"Unknown location: {location}")

    if days > 90:
        raise ValueError("Open-Meteo supports past_days up to 90.")

    lat, lon = LOCATIONS[location]

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,dust,aerosol_optical_depth",
        "past_days": days,
        "forecast_days": 0,
        "timezone": "UTC",
    }

    logger.info(f"[API] Requesting {days} days of history for {location}")

    r = requests.get(FORECAST_URL, params=params, timeout=15)
    r.raise_for_status()

    data = r.json()["hourly"]

    return AirQualityData(
        time=_to_datetime_list(data["time"]),
        pm10=data["pm10"],
        pm25=data["pm2_5"],
        dust=data["dust"],
        aod=data["aerosol_optical_depth"],
    )


# ----------------------------------------------------------
# Update: past 2 days + forecast 3 days
# ----------------------------------------------------------


def fetch_update(location: str) -> AirQualityData:
    """
    Fetch recent and forecast air-quality data.

    The returned dataset includes:
    - Past 2 days (historical)
    - Next 3 days (forecast)

    Args:
        location: Location key defined in LOCATIONS.

    Returns:
        AirQualityData containing recent and forecast measurements.

    Raises:
        ValueError: If the location is unknown.
        requests.HTTPError: If the API request fails.
    """
    if location not in LOCATIONS:
        raise ValueError(f"Unknown location: {location}")

    lat, lon = LOCATIONS[location]

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,dust,aerosol_optical_depth",
        "past_days": 2,
        "forecast_days": 3,
        "timezone": "UTC",
    }

    logger.info(f"[API] Requesting latest update for {location}")

    r = requests.get(FORECAST_URL, params=params, timeout=15)
    r.raise_for_status()

    data = r.json()["hourly"]

    return AirQualityData(
        time=_to_datetime_list(data["time"]),
        pm10=data["pm10"],
        pm25=data["pm2_5"],
        dust=data["dust"],
        aod=data["aerosol_optical_depth"],
    )


# ----------------------------------------------------------
# Retrieve current "closest to now" datapoint
# ----------------------------------------------------------


def get_current_point(location: str) -> CurrentPoint:
    """
    Retrieve the air-quality datapoint closest to the current UTC time.

    A simple Calima heuristic is applied:
    - dust > 150
    - OR pm10 > 50 AND aod > 0.5

    Args:
        location: Location key defined in LOCATIONS.

    Returns:
        CurrentPoint representing the closest datapoint to now.

    Raises:
        ValueError: If the location is unknown.
        requests.HTTPError: If the API request fails.
    """
    logger.info(f"[API] Fetching current air-quality point for {location}")

    aq = fetch_update(location)

    now = datetime.utcnow()
    i = _nearest_index(aq.time, now)

    pm10 = aq.pm10[i]
    pm25 = aq.pm25[i]
    dust = aq.dust[i]
    aod = aq.aod[i]

    is_calima = (
        (dust is not None and dust > 150)
        or (pm10 is not None and pm10 > 50 and aod is not None and aod > 0.5)
    )

    logger.info(
        f"[API] Current datapoint for {location}: "
        f"pm10={pm10}, pm25={pm25}, dust={dust}, aod={aod}, calima={is_calima}"
    )

    return CurrentPoint(
        location=location,
        time=aq.time[i],
        pm10=pm10,
        pm25=pm25,
        dust=dust,
        aod=aod,
        is_calima=is_calima,
    )


if __name__ == "__main__":
    print(get_current_point("santa_cruz"))
