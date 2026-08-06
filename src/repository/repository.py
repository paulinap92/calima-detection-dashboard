"""
CQRS repository layer for air-quality measurements and calima detection.
"""

import logging
from datetime import datetime

from mongoengine import DoesNotExist
from pymongo import UpdateOne

from src.repository.model import (
    AirLocation,
    AirQualityData,
    AirMeasurement,
    CalimaEvent,
)

logger = logging.getLogger(__name__)


class ModifyAirRepository:
    """Repository responsible for database write operations."""

    def add_location(self, name: str, lat: float, lon: float) -> AirLocation:
        loc = AirLocation(name=name, latitude=lat, longitude=lon)
        loc.save()
        logger.info("[COMMAND] New location created: %s", loc)
        return loc

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
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.error(
                "[COMMAND] Cannot add measurement: unknown location '%s'",
                location_name,
            )
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
        logger.info(
            "[COMMAND] Saved measurement for '%s' at %s",
            location_name,
            timestamp,
        )
        return measurement

    def bulk_add_measurements(
        self,
        location_name: str,
        measurements: list[AirQualityData],
    ) -> int:
        """Insert missing hours and repair existing incomplete hours.

        Each operation is an upsert keyed by ``(location, timestamp)``.
        Non-null API values update the stored document, while null API values
        never overwrite values that are already present.

        Returns:
            Number of inserted or modified documents.
        """
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.error(
                "[COMMAND] Cannot bulk upsert: unknown location '%s'",
                location_name,
            )
            return 0

        operations: list[UpdateOne] = []

        for measurement in measurements:
            values = {
                "pm10": measurement.pm10,
                "pm25": measurement.pm25,
                "dust": measurement.dust,
                "aod": measurement.aod,
            }

            # Do not create permanently empty documents. They produce visible
            # breaks in the charts and can be retried on a later API response.
            if not any(value is not None for value in values.values()):
                continue

            set_fields: dict[str, object] = {
                "data.is_calima": bool(measurement.is_calima),
            }
            for field_name, value in values.items():
                if value is not None:
                    set_fields[f"data.{field_name}"] = value

            operations.append(
                UpdateOne(
                    {
                        "location": loc.id,
                        "data.timestamp": measurement.timestamp,
                    },
                    {"$set": set_fields},
                    upsert=True,
                )
            )

        if not operations:
            return 0

        result = AirMeasurement._get_collection().bulk_write(
            operations,
            ordered=False,
        )
        written = result.upserted_count + result.modified_count

        logger.info(
            "[COMMAND] Upserted %s and repaired %s records for '%s' "
            "(matched=%s)",
            result.upserted_count,
            result.modified_count,
            location_name,
            result.matched_count,
        )
        return written

    def delete_measurements_for_location(self, location_name: str) -> int:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.warning(
                "[COMMAND] No measurements deleted — location '%s' does not exist",
                location_name,
            )
            return 0

        count = AirMeasurement.objects(location=loc).delete()
        logger.info(
            "[COMMAND] Deleted %s measurements for location '%s'",
            count,
            location_name,
        )
        return count

    def update_measurement(
        self,
        measurement_id: str,
        **kwargs,
    ) -> AirMeasurement:
        try:
            measurement = AirMeasurement.objects.get(id=measurement_id)
        except DoesNotExist as exc:
            raise ValueError(
                f"Measurement {measurement_id} not found"
            ) from exc

        valid_fields = ["pm10", "pm25", "dust", "aod", "is_calima"]
        update_dict = {
            field: kwargs[field]
            for field in valid_fields
            if field in kwargs and kwargs[field] is not None
        }

        if not update_dict:
            raise ValueError(
                f"No valid fields to update for measurement {measurement_id}"
            )

        for key, value in update_dict.items():
            setattr(measurement.data, key, value)

        measurement.save()
        logger.info(
            "[COMMAND] Updated measurement %s: %s",
            measurement_id,
            update_dict,
        )
        return measurement

    def add_calima_event(
        self,
        location_name: str,
        start: datetime,
        end: datetime,
        peak_pm10: float | None,
        peak_dust: float | None,
        peak_aod: float | None,
    ) -> CalimaEvent | None:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            logger.error(
                "[COMMAND] Cannot save event: unknown location '%s'",
                location_name,
            )
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
        logger.info("[COMMAND] Calima event stored: %s", event)
        return event


class ReadAirRepository:
    """Repository responsible exclusively for read operations."""

    def get_measurements(
        self,
        location_name: str,
    ) -> list[AirMeasurement]:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(
            AirMeasurement.objects(location=loc).order_by("data.timestamp")
        )

    def get_latest(
        self,
        location_name: str,
    ) -> AirMeasurement | None:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return None
        return (
            AirMeasurement.objects(location=loc)
            .order_by("-data.timestamp")
            .first()
        )

    def get_range(
        self,
        location_name: str,
        start: datetime,
        end: datetime,
    ) -> list[AirMeasurement]:
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

    def find_calima_hours(
        self,
        location_name: str,
    ) -> list[AirMeasurement]:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(
            AirMeasurement.objects(
                location=loc,
                data__is_calima=True,
            ).order_by("data.timestamp")
        )

    def get_daily_avg(self, location_name: str) -> list[dict]:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []

        pipeline = [
            {"$match": {"location": loc.id}},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "date": "$data.timestamp",
                            "format": "%Y-%m-%d",
                        }
                    },
                    "pm10": {"$avg": "$data.pm10"},
                    "pm25": {"$avg": "$data.pm25"},
                    "dust": {"$avg": "$data.dust"},
                    "aod": {"$avg": "$data.aod"},
                }
            },
            {"$sort": {"_id": 1}},
        ]
        return list(AirMeasurement.objects.aggregate(*pipeline))

    def get_daily_max(self, location_name: str) -> list[dict]:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []

        pipeline = [
            {"$match": {"location": loc.id}},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "date": "$data.timestamp",
                            "format": "%Y-%m-%d",
                        }
                    },
                    "pm10": {"$max": "$data.pm10"},
                    "pm25": {"$max": "$data.pm25"},
                    "dust": {"$max": "$data.dust"},
                    "aod": {"$max": "$data.aod"},
                }
            },
            {"$sort": {"_id": 1}},
        ]
        return list(AirMeasurement.objects.aggregate(*pipeline))

    def get_calima_events(self, location_name: str):
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(
            CalimaEvent.objects(location=loc).order_by("-start_time")
        )

    def get_events_over_threshold(
        self,
        location_name: str,
        pm10_min: float,
    ):
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return []
        return list(
            CalimaEvent.objects(
                location=loc,
                peak_pm10__gte=pm10_min,
            ).order_by("-start_time")
        )

    def get_existing_timestamps(
        self,
        location_name: str,
        start: datetime,
        end: datetime,
    ) -> set[datetime]:
        try:
            loc = AirLocation.objects.get(name=location_name)
        except DoesNotExist:
            return set()

        measurements = AirMeasurement.objects(
            location=loc,
            data__timestamp__gte=start,
            data__timestamp__lte=end,
        ).only("data.timestamp")
        return {measurement.data.timestamp for measurement in measurements}
