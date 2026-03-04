"""
Dashboard data-access and transformation utilities.

This module provides small helper functions used by the dashboard layer to:
- connect/disconnect from MongoDB (via MongoEngine)
- create repository and updater instances
- load domain objects (locations, measurements, daily aggregates, calima events)
- build a Pandas DataFrame for map visualization (latest point per location)

The functions here are intentionally simple and side-effect-light.
They are designed to be called from Streamlit or other UI entrypoints.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.repository.db_config import connect_nosql_db, disconnect_nosql_db
from src.repository.repository import ReadAirRepository
from src.service.update_service import UpdateService
from src.repository.model import AirLocation

from src.dashboard.domain.severity import compute_severity, severity_label, severity_color


def connect_db() -> None:
    """Connect to the MongoDB database using the project's NoSQL config."""
    connect_nosql_db()


def disconnect_db() -> None:
    """Disconnect from the MongoDB database."""
    disconnect_nosql_db()


def safe_disconnect() -> None:
    """
    Attempt to disconnect from MongoDB without raising errors.

    This is useful for UI frameworks (e.g., Streamlit) or shutdown hooks where
    disconnect failures should not crash the process.
    """
    try:
        disconnect_db()
    except Exception:
        pass


def make_repo() -> ReadAirRepository:
    """
    Create a read-only repository instance.

    Returns:
        ReadAirRepository instance bound to the current MongoEngine connection.
    """
    return ReadAirRepository()


def make_updater() -> UpdateService:
    """
    Create an UpdateService instance.

    Returns:
        UpdateService instance.
    """
    return UpdateService()


def load_locations() -> list[Any]:
    """
    Load all locations from the database.

    Returns:
        List of AirLocation documents (typed as Any to avoid tight coupling
        between dashboard and ODM models).
    """
    return list(AirLocation.objects())


def load_measurements(repo: ReadAirRepository, location: str):
    """
    Load all measurements for a location.

    Args:
        repo: Read repository.
        location: Location identifier.

    Returns:
        List of measurement documents for the location.
    """
    return repo.get_measurements(location)


def load_daily(repo: ReadAirRepository, location: str):
    """
    Load daily averages for a location.

    Args:
        repo: Read repository.
        location: Location identifier.

    Returns:
        Daily aggregates for the location (repository-defined structure).
    """
    return repo.get_daily_avg(location)


def load_events(repo: ReadAirRepository, location: str):
    """
    Load detected calima events for a location.

    Args:
        repo: Read repository.
        location: Location identifier.

    Returns:
        List of CalimaEvent documents for the location.
    """
    return repo.get_calima_events(location)


def build_map_df(locations, repo: ReadAirRepository) -> pd.DataFrame:
    """
    Build a DataFrame for the dashboard map layer.

    For each location:
      - fetch measurements
      - take the latest measurement
      - compute severity and UI-friendly fields

    Rows are skipped if:
      - location has no latitude/longitude
      - there are no measurements
      - any unexpected exception occurs while processing a location

    Args:
        locations: Iterable of location documents/objects (expected to have
            `.name`, `.latitude`, `.longitude`).
        repo: Read repository used to load measurements.

    Returns:
        Pandas DataFrame with one row per location containing:
          - name, lat, lon
          - pm10, pm25, dust, aod
          - severity, status, color
          - metric_height (dust if available, otherwise pm10)
          - timestamp (string formatted as "%Y-%m-%d %H:%M")
    """
    rows = []
    for loc in locations:
        lat = getattr(loc, "latitude", None)
        lon = getattr(loc, "longitude", None)
        if lat is None or lon is None:
            continue

        try:
            ms = repo.get_measurements(loc.name)
            if not ms:
                continue

            last = ms[-1].data

            pm10 = float(last.pm10 or 0)
            pm25 = float(last.pm25 or 0)
            dust = float(last.dust or 0)
            aod = float(last.aod or 0)

            sev = compute_severity(pm10, pm25, dust, aod)

            rows.append(
                {
                    "name": loc.name,
                    "lat": float(lat),
                    "lon": float(lon),
                    "pm10": pm10,
                    "pm25": pm25,
                    "dust": dust,
                    "aod": aod,
                    "severity": sev,
                    "status": severity_label(sev),
                    "color": severity_color(sev),
                    "metric_height": dust if dust > 0 else pm10,
                    "timestamp": last.timestamp.strftime("%Y-%m-%d %H:%M"),
                }
            )
        except Exception:
            continue

    return pd.DataFrame(rows)
