import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mongoengine import connect, disconnect

from src.repository.model import AirLocation, AirMeasurement, CalimaEvent


MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "calima")
OUTPUT_PATH = Path(os.getenv("CALIMA_EXPORT_PATH", "static/calima_export.json"))


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def export_locations() -> list[dict]:
    docs = AirLocation.objects().only(
        "name",
        "latitude",
        "longitude",
        "created_at",
    )
    return [
        {
            "name": doc.name,
            "latitude": getattr(doc, "latitude", None),
            "longitude": getattr(doc, "longitude", None),
            "created_at": _to_json_safe(getattr(doc, "created_at", None)),
        }
        for doc in docs
    ]


def export_measurements() -> list[dict]:
    docs = (
        AirMeasurement.objects()
        .only("location", "data")
        .order_by("data.timestamp")
    )
    out: list[dict] = []
    for measurement in docs:
        location = (
            getattr(getattr(measurement, "location", None), "name", None)
            or str(measurement.location)
        )
        data = measurement.data
        out.append(
            {
                "location": location,
                "timestamp": _to_json_safe(
                    getattr(data, "timestamp", None)
                ),
                "pm10": getattr(data, "pm10", None),
                "pm25": getattr(data, "pm25", None),
                "dust": getattr(data, "dust", None),
                "aod": getattr(data, "aod", None),
                "is_calima": getattr(data, "is_calima", None),
            }
        )
    return out


def export_events() -> list[dict]:
    docs = (
        CalimaEvent.objects()
        .only(
            "location",
            "start_time",
            "end_time",
            "peak_pm10",
            "peak_dust",
            "peak_aod",
        )
        .order_by("-start_time")
    )
    out: list[dict] = []
    for event in docs:
        location = (
            getattr(getattr(event, "location", None), "name", None)
            or str(event.location)
        )
        out.append(
            {
                "location": location,
                "start_time": _to_json_safe(
                    getattr(event, "start_time", None)
                ),
                "end_time": _to_json_safe(
                    getattr(event, "end_time", None)
                ),
                "peak_pm10": getattr(event, "peak_pm10", None),
                "peak_dust": getattr(event, "peak_dust", None),
                "peak_aod": getattr(event, "peak_aod", None),
            }
        )
    return out


def main() -> None:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is not set")

    print(f"[EXPORT] Connecting to MongoDB Atlas db={DB_NAME}")
    connect(
        db=DB_NAME,
        host=MONGO_URI,
        uuidRepresentation="standard",
        serverSelectionTimeoutMS=5000,
    )

    try:
        payload = {
            "meta": {
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "db_name": DB_NAME,
                "source": "calima-detection-dashboard",
            },
            "locations": export_locations(),
            "measurements": export_measurements(),
            "events": export_events(),
        }

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
                default=_to_json_safe,
            ),
            encoding="utf-8",
        )

        print(
            f"[EXPORT] Done -> {OUTPUT_PATH}\n"
            f"  locations: {len(payload['locations'])}\n"
            f"  measurements: {len(payload['measurements'])}\n"
            f"  events: {len(payload['events'])}"
        )
    finally:
        disconnect()
        print("[EXPORT] Disconnected")


if __name__ == "__main__":
    main()
