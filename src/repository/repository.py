"""
CQRS repository layer for air-quality measurements and calima detection.

This module implements two repository classes that follow the
Command Query Responsibility Segregation (CQRS) pattern:

- ModifyAirRepository (COMMAND side):
    Handles write operations such as inserting new measurements,
    updating values, bulk imports, deleting records, and creating
    CalimaEvent entries.

- ReadAirRepository (QUERY side):
    Contains only read operations. It exposes queries such as
    fetching measurements, daily aggregations, and retrieving
    calima events.

No read method mutates the database, and no write method returns
derived analytical data. This separation keeps the logic clean,
testable, and aligned with CQRS principles.

MongoEngine models used:
    - AirLocation
    - AirMeasurement
    - AirQualityData (embedded)
    - CalimaEvent
"""

from mongoengine import DoesNotExist, Q
from datetime import datetime
import logging

from src.repository.model import (
    AirLocation,
    AirQualityData,
    AirMeasurement,
    CalimaEvent
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
#                     MODIFY (COMMAND)
# ============================================================

class ModifyAirRepository:
    """
    Repository responsible for all database write operations.
    This includes adding locations, inserting measurements,
    bulk-inserting historical data, updating existing entries,
    deleting measurements, and creating calima events.

    Following CQRS, no read/query logic is implemented here.
    """

    # ---------------------------
    # Add a new location
    # ---------------------------
    def add_location(self, name: str, lat: float, lon: float) -> AirLocation:
        """Insert a new AirLocation document.

        Args:
            name: Unique location name.
            lat: Latitude.
            lon: Longitude.

        Returns:
            The created AirLocation document.
        """
        loc = AirLocation(name=name, latitude=lat, longitude=lon)
        loc.save()
        logger.info(f"[COMMAND] New location created: {loc}")
        return loc

    # ---------------------------
    # Add a single hourly measurement
    # ---------------------------
    def add_measurement(
        self,
        location_name: str,
        timestamp: datetime,
        pm10: float | None,
        pm25: float | None,
        dust: float | None,
        aod: float | None,
        is_calima: bool,
    ) -> AirMeasurement | None:
        """Insert a single hourly AirMeasurement for a given location."""

        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.error(f"[COMMAND] Cannot add measurement: unknown location '{location_name}'")
            return None

        data = AirQualityData(
            timestamp=timestamp,
            pm10=pm10,
            pm25=pm25,
            dust=dust,
            aod=aod,
            is_calima=is_calima,
        )

        measurement = AirMeasurement(location=loc, data=data)
        measurement.save()

        logger.info(f"[COMMAND] Saved measurement for '{location_name}' at {timestamp}")
        return measurement

    # ---------------------------
    # Bulk insert historical data
    # ---------------------------
    from mongoengine.errors import NotUniqueError, BulkWriteError

    def bulk_add_measurements(self, location_name: str, measurements: list[AirQualityData]) -> int:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.error(f"[COMMAND] Cannot bulk insert: unknown location '{location_name}'")
            return 0

        docs = [AirMeasurement(location=loc, data=m) for m in measurements]

        if not docs:
            return 0

        try:
            AirMeasurement.objects.insert(docs, load_bulk=False)
            inserted = len(docs)
        except Exception as e:
            # Mongo zwróci BulkWriteError, jeśli choć jeden dokument to duplikat
            logger.warning(f"[COMMAND] Bulk insert partially failed (duplicates ignored): {e}")
            inserted = 0  # w wersji perfect można policzyć ile weszło

        logger.info(f"[COMMAND] Bulk inserted {inserted} new records for '{location_name}'")
        return inserted

    # ---------------------------
    # Delete all measurements for a location
    # ---------------------------
    def delete_measurements_for_location(self, location_name: str) -> int:
        """Delete all AirMeasurement documents for a given location."""

        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.warning(f"[COMMAND] No measurements deleted — location '{location_name}' does not exist")
            return 0

        count = AirMeasurement.objects(location=loc).delete()
        logger.info(f"[COMMAND] Deleted {count} measurements for location '{location_name}'")
        return count

    # ---------------------------
    # Update a measurement
    # ---------------------------
    def update_measurement(self, measurement_id: str, **kwargs) -> AirMeasurement:
        """Update selected fields of AirQualityData embedded in AirMeasurement."""

        try:
            measurement = AirMeasurement.objects.get(id=measurement_id)
        except DoesNotExist:
            raise ValueError(f"Measurement {measurement_id} not found")

        valid_fields = ["pm10", "pm25", "dust", "aod", "is_calima"]
        update_dict = {}

        for field in valid_fields:
            if field in kwargs and kwargs[field] is not None:
                update_dict[field] = kwargs[field]

        if not update_dict:
            raise ValueError(f"No valid fields to update for measurement {measurement_id}")

        for key, value in update_dict.items():
            setattr(measurement.data, key, value)

        measurement.save()

        logger.info(f"[COMMAND] Updated measurement {measurement_id}: {update_dict}")
        return measurement

    # ---------------------------
    # Store a detected calima event
    # ---------------------------
    def add_calima_event(
        self,
        location_name: str,
        start: datetime,
        end: datetime,
        peak_pm10: float | None,
        peak_dust: float | None,
        peak_aod: float | None,
    ) -> CalimaEvent | None:
        """Create a CalimaEvent document for a given location."""

        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.error(f"[COMMAND] Cannot save event: unknown location '{location_name}'")
            return None

        event = CalimaEvent(
            location=loc,
            start_time=start,
            end_time=end,
            peak_pm10=peak_pm10,
            peak_dust=peak_dust,
            peak_aod=peak_aod,
        )
        event.save()

        logger.info(f"[COMMAND] Calima event stored: {event}")
        return event


# ============================================================
#                     READ (QUERY)
# ============================================================

class ReadAirRepository:
    """
    Repository responsible exclusively for read operations.
    It contains no write logic, following CQRS principles.
    """

    def get_measurements(self, location_name: str) -> list[AirMeasurement]:
        """Return all measurements for a given location, sorted by timestamp."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(AirMeasurement.objects(location=loc).order_by("data.timestamp"))

    def get_latest(self, location_name: str) -> AirMeasurement | None:
        """Return the most recent measurement for a location."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return None
        return AirMeasurement.objects(location=loc).order_by("-data.timestamp").first()

    def get_range(self, location_name: str, start: datetime, end: datetime) -> list[AirMeasurement]:
        """Return measurements within a time range."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(
            AirMeasurement.objects(
                location=loc,
                data__timestamp__gte=start,
                data__timestamp__lte=end,
            ).order_by("data.timestamp")
        )

    def find_calima_hours(self, location_name: str) -> list[AirMeasurement]:
        """Return all measurements marked as calima=True."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(
            AirMeasurement.objects(
                location=loc, data__is_calima=True
            ).order_by("data.timestamp")
        )

    def get_daily_avg(self, location_name: str) -> list[dict]:
        """Return daily averages of PM10, PM25, dust, and AOD."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []

        pipeline = [
            {"$match": {"location": loc.id}},
            {"$group": {
                "_id": {"$dateToString": {"date": "$data.timestamp", "format": "%Y-%m-%d"}},
                "pm10": {"$avg": "$data.pm10"},
                "pm25": {"$avg": "$data.pm25"},
                "dust": {"$avg": "$data.dust"},
                "aod": {"$avg": "$data.aod"},
            }},
            {"$sort": {"_id": 1}}
        ]

        return list(AirMeasurement.objects.aggregate(*pipeline))

    def get_daily_max(self, location_name: str) -> list[dict]:
        """Return daily maximum values for PM10, PM25, dust, and AOD."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []

        pipeline = [
            {"$match": {"location": loc.id}},
            {"$group": {
                "_id": {"$dateToString": {"date": "$data.timestamp", "format": "%Y-%m-%d"}},
                "pm10": {"$max": "$data.pm10"},
                "pm25": {"$max": "$data.pm25"},
                "dust": {"$max": "$data.dust"},
                "aod": {"$max": "$data.aod"},
            }},
            {"$sort": {"_id": 1}}
        ]

        return list(AirMeasurement.objects.aggregate(*pipeline))

    def get_calima_events(self, location_name: str):
        """Return all calima events for a location (newest first)."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(CalimaEvent.objects(location=loc).order_by("-start_time"))

    def get_events_over_threshold(self, location_name: str, pm10_min: float):
        """Return all events where peak PM10 exceeds a threshold."""
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []

        return list(
            CalimaEvent.objects(
                location=loc,
                peak_pm10__gte=pm10_min
            ).order_by("-start_time")
        )
