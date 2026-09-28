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


def _location_rows() -> tuple[list[dict[str, Any]], dict[str, str]]:
    docs = list(
        AirLocation._get_collection().find(
            {},
            {
                "name": 1,
                "latitude": 1,
                "longitude": 1,
                "created_at": 1,
            },
        )
    )
    locations = [
        {
            "name": doc.get("name"),
            "latitude": doc.get("latitude"),
            "longitude": doc.get("longitude"),
            "created_at": _to_json_safe(doc.get("created_at")),
        }
        for doc in docs
    ]
    by_id = {
        str(doc["_id"]): str(doc.get("name") or doc["_id"])
        for doc in docs
        if doc.get("_id") is not None
    }
    return locations, by_id


def _location_name(value: Any, by_id: dict[str, str]) -> str:
    if value is None:
        return ""
    # MongoEngine ReferenceField is normally stored as an ObjectId. Handle
    # DBRef defensively as well for older documents.
    ref_id = getattr(value, "id", value)
    return by_id.get(str(ref_id), str(ref_id))


def export_measurements(by_id: dict[str, str]) -> list[dict[str, Any]]:
    cursor = (
        AirMeasurement._get_collection()
        .find(
            {},
            {
                "location": 1,
                "data.timestamp": 1,
                "data.pm10": 1,
                "data.pm25": 1,
                "data.dust": 1,
                "data.aod": 1,
                "data.is_calima": 1,
            },
        )
        .sort("data.timestamp", 1)
    )
    out: list[dict[str, Any]] = []
    for doc in cursor:
        data = doc.get("data") or {}
        out.append(
            {
                "location": _location_name(doc.get("location"), by_id),
                "timestamp": _to_json_safe(data.get("timestamp")),
                "pm10": data.get("pm10"),
                "pm25": data.get("pm25"),
                "dust": data.get("dust"),
                "aod": data.get("aod"),
                "is_calima": data.get("is_calima"),
            }
        )
    return out


def export_events(by_id: dict[str, str]) -> list[dict[str, Any]]:
    cursor = (
        CalimaEvent._get_collection()
        .find(
            {},
            {
                "location": 1,
                "start_time": 1,
                "end_time": 1,
                "peak_pm10": 1,
                "peak_dust": 1,
                "peak_aod": 1,
            },
        )
        .sort("start_time", -1)
    )
    out: list[dict[str, Any]] = []
    for doc in cursor:
        out.append(
            {
                "location": _location_name(doc.get("location"), by_id),
                "start_time": _to_json_safe(doc.get("start_time")),
                "end_time": _to_json_safe(doc.get("end_time")),
                "peak_pm10": doc.get("peak_pm10"),
                "peak_dust": doc.get("peak_dust"),
                "peak_aod": doc.get("peak_aod"),
            }
        )
    return out


def main() -> None:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is not set")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"[EXPORT] Connecting to MongoDB Atlas db={DB_NAME}", flush=True)
    connect(
        db=DB_NAME,
        host=MONGO_URI,
        uuidRepresentation="standard",
        serverSelectionTimeoutMS=5000,
    )

    try:
        locations, location_by_id = _location_rows()

        measurement_count = AirMeasurement._get_collection().count_documents({})
        event_count = CalimaEvent._get_collection().count_documents({})
        print(
            f"[EXPORT] Found locations={len(locations)} "
            f"measurements={measurement_count} events={event_count}",
            flush=True,
        )

        measurements = export_measurements(location_by_id)
        events = export_events(location_by_id)

        payload = {
            "meta": {
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "db_name": DB_NAME,
                "source": "calima-detection-dashboard",
            },
            "locations": locations,
            "measurements": measurements,
            "events": events,
        }

        OUTPUT_PATH.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                default=_to_json_safe,
            ),
            encoding="utf-8",
        )

        print(
            f"[EXPORT] Done -> {OUTPUT_PATH} "
            f"locations={len(locations)} "
            f"measurements={len(measurements)} "
            f"events={len(events)}",
            flush=True,
        )
    finally:
        disconnect()
        print("[EXPORT] Disconnected", flush=True)


if __name__ == "__main__":
    main()
