import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mongoengine import connect, disconnect

# Dopasuj import jeśli masz inne ścieżki:
from src.repository.model import AirLocation, AirMeasurement, CalimaEvent


# =============================
# HARDCODED SETTINGS (LOCAL ONLY)
# =============================
MONGO_URI = "mongodb://localhost:27018"   # host -> docker mapped port
DB_NAME = "calima"
OUTPUT_PATH = Path("calima_export.json")


def _to_json_safe(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def export_locations() -> list[dict]:
    docs = AirLocation.objects().only("name", "latitude", "longitude", "created_at")
    out: list[dict] = []

    for d in docs:
        out.append(
            {
                "name": d.name,
                "latitude": getattr(d, "latitude", None),
                "longitude": getattr(d, "longitude", None),
                "created_at": _to_json_safe(getattr(d, "created_at", None)),
            }
        )
    return out


def export_measurements() -> list[dict]:
    docs = AirMeasurement.objects().only("location", "data").order_by("data.timestamp")
    out: list[dict] = []

    for m in docs:
        loc_name = getattr(getattr(m, "location", None), "name", None) or str(m.location)
        d = m.data

        out.append(
            {
                "location": loc_name,
                "timestamp": _to_json_safe(getattr(d, "timestamp", None)),
                "pm10": getattr(d, "pm10", None),
                "pm25": getattr(d, "pm25", None),
                "dust": getattr(d, "dust", None),
                "aod": getattr(d, "aod", None),
                "is_calima": getattr(d, "is_calima", None),
            }
        )
    return out


def export_events() -> list[dict]:
    docs = (
        CalimaEvent.objects()
        .only("location", "start_time", "end_time", "peak_pm10", "peak_dust", "peak_aod")
        .order_by("-start_time")
    )

    out: list[dict] = []
    for e in docs:
        loc_name = getattr(getattr(e, "location", None), "name", None) or str(e.location)

        out.append(
            {
                "location": loc_name,
                "start_time": _to_json_safe(getattr(e, "start_time", None)),
                "end_time": _to_json_safe(getattr(e, "end_time", None)),
                "peak_pm10": getattr(e, "peak_pm10", None),
                "peak_dust": getattr(e, "peak_dust", None),
                "peak_aod": getattr(e, "peak_aod", None),
            }
        )
    return out


def main() -> None:
    print(f"[EXPORT] Connecting to {MONGO_URI} db={DB_NAME}")
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
                "mongo_uri": MONGO_URI,
                "db_name": DB_NAME,
            },
            "locations": export_locations(),
            "measurements": export_measurements(),
            "events": export_events(),
        }

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with OUTPUT_PATH.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=_to_json_safe)

        print(
            f"[EXPORT] Done -> {OUTPUT_PATH.resolve()}\n"
            f"  locations: {len(payload['locations'])}\n"
            f"  measurements: {len(payload['measurements'])}\n"
            f"  events: {len(payload['events'])}"
        )

    finally:
        disconnect()
        print("[EXPORT] Disconnected")


if __name__ == "__main__":
    main()
