"""
MongoEngine data models for Canary Islands Air Quality and Calima Detection.

This module defines the core documents used in the NoSQL database:

- AirLocation:
    Represents a measurement site with fixed geographic coordinates.
    Contains metadata about when the location was created.
    It is referenced by both AirMeasurement and CalimaEvent.

- AirQualityData:
    Embedded hourly air-quality measurement (PM10, PM2.5, dust, AOD).
    This is *not* a standalone document — it only exists inside AirMeasurement.

- AirMeasurement:
    A single hourly measurement referencing a location and holding
    an embedded AirQualityData object.

- CalimaEvent:
    Represents a detected Saharan dust episode (“calima”).
    Detected through the CalimaDetector, based on thresholds and
    continuous time windows.

Delete rules:
    - Removing an AirLocation cascades into deleting all AirMeasurements
      and CalimaEvents that reference it.

All timestamps are stored in timezone-aware UTC datetimes.
"""

from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    ReferenceField,
    StringField,
    FloatField,
    DateTimeField,
    BooleanField,
    ListField,
    CASCADE
)
import datetime


# ---------------------------------------------------
# 1) Location of air-quality measurements
# ---------------------------------------------------

class AirLocation(Document):
    """Represents a measurement location with geographic coordinates.

    Attributes:
        name (str): Unique name of the location (e.g. 'santa_cruz').
        latitude (float): Geographic latitude.
        longitude (float): Geographic longitude.
        created_at (datetime): Creation timestamp (UTC).

    Notes:
        - Locations must have unique names.
        - Deleting a location removes all measurements and calima events
          referencing it (CASCADE).
    """

    name = StringField(required=True, unique=True)
    latitude = FloatField(required=True)
    longitude = FloatField(required=True)
    created_at = DateTimeField(default=datetime.datetime.now(datetime.timezone.utc))

    def __repr__(self) -> str:  # pragma: no cover
        return f"AirLocation(name={self.name}, lat={self.latitude}, lon={self.longitude})"


# ---------------------------------------------------
# 2) Embedded hourly air-quality data
# ---------------------------------------------------

class AirQualityData(EmbeddedDocument):
    """Embedded hourly measurement returned by the scheduler or the API.

    Attributes:
        timestamp (datetime): Timestamp of the hourly measurement (UTC).
        pm10 (float|None): PM10 concentration.
        pm25 (float|None): PM2.5 concentration.
        dust (float|None): Desert dust concentration proxy.
        aod (float|None): Aerosol optical depth.
        is_calima (bool): Whether this hour meets calima heuristics.

    Notes:
        - This class is embedded inside AirMeasurement.
        - It is never stored as a standalone MongoDB document.
    """

    timestamp = DateTimeField(required=True)
    pm10 = FloatField()
    pm25 = FloatField()
    dust = FloatField()
    aod = FloatField()
    is_calima = BooleanField(default=False)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"AirQualityData(ts={self.timestamp}, pm10={self.pm10}, "
            f"pm25={self.pm25}, dust={self.dust}, aod={self.aod}, calima={self.is_calima})"
        )


# ---------------------------------------------------
# 3) A stored hourly measurement
# ---------------------------------------------------

class AirMeasurement(Document):
    """Stores a single hourly measurement associated with a location.

    Attributes:
        location (AirLocation): The referenced measurement site.
        data (AirQualityData): Embedded air-quality details.

    Delete behavior:
        - If the linked AirLocation is removed,
          all its AirMeasurements are also removed (CASCADE).
    """

    location = ReferenceField(AirLocation, required=True, reverse_delete_rule=CASCADE)
    data = EmbeddedDocumentField(AirQualityData, required=True)
    meta = {
        "indexes": [
            {
                "fields": ["location", "data.timestamp"],
                "unique": True,  # ← kluczowy indeks!
            }
        ]
    }
    def __repr__(self) -> str:  # pragma: no cover
        return f"AirMeasurement(loc={self.location.name}, {self.data})"


# ---------------------------------------------------
# 4) Detected Saharan dust event (calima)
# ---------------------------------------------------

class CalimaEvent(Document):
    """Represents a detected calima event — a multi-hour dust episode.

    Attributes:
        location (AirLocation): The site where the event occurred.
        start_time (datetime): Start of the event (UTC).
        end_time (datetime): End of the event (UTC).
        peak_pm10 (float|None): Maximum PM10 during the episode.
        peak_dust (float|None): Maximum dust concentration during the episode.
        peak_aod (float|None): Maximum aerosol optical depth during the episode.

    Notes:
        - Created via CalimaDetector.detect_events().
        - An event is only stored if it lasts ≥ 3 hours.
        - Deleting the related AirLocation removes all events as well.
    """

    location = ReferenceField(AirLocation, required=True, reverse_delete_rule=CASCADE)
    start_time = DateTimeField(required=True)
    end_time = DateTimeField(required=True)
    peak_pm10 = FloatField()
    peak_dust = FloatField()
    peak_aod = FloatField()

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CalimaEvent(loc={self.location.name}, "
            f"{self.start_time} → {self.end_time}, "
            f"PM10_peak={self.peak_pm10}, dust_peak={self.peak_dust}, aod_peak={self.peak_aod})"
        )
