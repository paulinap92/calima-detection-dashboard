from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.repository.repository import ModifyAirRepository, ReadAirRepository
from src.repository.model import AirLocation, AirMeasurement, AirQualityData, CalimaEvent


def dt(hours: int) -> datetime:
    """
    Use naive datetimes in tests because MongoEngine/mongomock commonly returns
    naive datetimes even if tz-aware were stored.
    """
    return datetime(2026, 1, 1, 0, 0, 0) + timedelta(hours=hours)


@pytest.fixture()
def modify_repo() -> ModifyAirRepository:
    return ModifyAirRepository()


@pytest.fixture()
def read_repo() -> ReadAirRepository:
    return ReadAirRepository()


@pytest.fixture()
def location(modify_repo: ModifyAirRepository) -> AirLocation:
    return modify_repo.add_location("santa_cruz", 28.4636, -16.2518)


# -------------------------
# ModifyAirRepository tests
# -------------------------

def test_add_location_creates_document(modify_repo: ModifyAirRepository):
    loc = modify_repo.add_location("adeje", 28.1227, -16.7260)

    assert loc.id is not None
    assert loc.name == "adeje"

    loaded = AirLocation.objects.get(name="adeje")
    assert loaded.latitude == pytest.approx(28.1227)
    assert loaded.longitude == pytest.approx(-16.7260)


def test_add_measurement_returns_none_for_unknown_location(modify_repo: ModifyAirRepository):
    m = modify_repo.add_measurement(
        location_name="unknown",
        timestamp=dt(0),
        pm10=10.0,
        pm25=5.0,
        dust=1.0,
        aod=0.1,
        is_calima=False,
    )
    assert m is None
    assert AirMeasurement.objects.count() == 0


def test_add_measurement_saves_document(modify_repo: ModifyAirRepository, location: AirLocation):
    m = modify_repo.add_measurement(
        location_name="santa_cruz",
        timestamp=dt(0),
        pm10=40.0,
        pm25=15.0,
        dust=120.0,
        aod=0.6,
        is_calima=True,
    )

    assert m is not None
    assert m.id is not None
    assert m.location.id == location.id
    assert m.data.timestamp == dt(0)
    assert m.data.pm10 == 40.0
    assert m.data.is_calima is True

    assert AirMeasurement.objects.count() == 1


def test_bulk_add_measurements_unknown_location_returns_0(modify_repo: ModifyAirRepository):
    inserted = modify_repo.bulk_add_measurements(
        "unknown",
        measurements=[AirQualityData(timestamp=dt(0), pm10=1, pm25=1, dust=1, aod=0.1, is_calima=False)],
    )
    assert inserted == 0
    assert AirMeasurement.objects.count() == 0


def test_bulk_add_measurements_empty_list_returns_0(modify_repo: ModifyAirRepository, location: AirLocation):
    inserted = modify_repo.bulk_add_measurements("santa_cruz", measurements=[])
    assert inserted == 0
    assert AirMeasurement.objects.count() == 0


def test_bulk_add_measurements_inserts_all(modify_repo: ModifyAirRepository, location: AirLocation):
    data = [
        AirQualityData(timestamp=dt(0), pm10=10, pm25=5, dust=50, aod=0.2, is_calima=False),
        AirQualityData(timestamp=dt(1), pm10=30, pm25=10, dust=200, aod=0.9, is_calima=True),
        AirQualityData(timestamp=dt(2), pm10=20, pm25=8, dust=120, aod=0.5, is_calima=False),
    ]
    inserted = modify_repo.bulk_add_measurements("santa_cruz", measurements=data)

    assert inserted == 3
    assert AirMeasurement.objects.count() == 3


def test_bulk_add_measurements_exception_path_returns_0(
    monkeypatch,
    modify_repo: ModifyAirRepository,
    location: AirLocation,
):
    """
    Patch QuerySet.insert (not the manager instance) because AirMeasurement.objects
    often returns a fresh QuerySet; patching the manager won't reliably intercept.
    """
    def boom(*args, **kwargs):
        raise Exception("BulkWriteError simulated")

    monkeypatch.setattr(AirMeasurement.objects.__class__, "insert", boom, raising=False)

    inserted = modify_repo.bulk_add_measurements(
        "santa_cruz",
        measurements=[AirQualityData(timestamp=dt(0), pm10=1, pm25=1, dust=1, aod=0.1, is_calima=False)],
    )
    assert inserted == 0


def test_delete_measurements_for_location_unknown_location_returns_0(modify_repo: ModifyAirRepository):
    deleted = modify_repo.delete_measurements_for_location("unknown")
    assert deleted == 0


def test_delete_measurements_for_location_deletes_all(modify_repo: ModifyAirRepository, location: AirLocation):
    modify_repo.add_measurement("santa_cruz", dt(0), 10, 5, 50, 0.2, False)
    modify_repo.add_measurement("santa_cruz", dt(1), 20, 7, 80, 0.3, False)

    assert AirMeasurement.objects.count() == 2

    deleted = modify_repo.delete_measurements_for_location("santa_cruz")
    assert deleted == 2
    assert AirMeasurement.objects.count() == 0


def test_update_measurement_unknown_id_raises_value_error(modify_repo: ModifyAirRepository):
    with pytest.raises(ValueError, match="not found"):
        modify_repo.update_measurement("000000000000000000000000", pm10=99.0)


def test_update_measurement_no_valid_fields_raises_value_error(modify_repo: ModifyAirRepository, location: AirLocation):
    m = modify_repo.add_measurement("santa_cruz", dt(0), 10, 5, 50, 0.2, False)
    assert m is not None

    with pytest.raises(ValueError, match="No valid fields to update"):
        modify_repo.update_measurement(str(m.id))


def test_update_measurement_updates_only_non_none_fields(modify_repo: ModifyAirRepository, location: AirLocation):
    m = modify_repo.add_measurement("santa_cruz", dt(0), 10, 5, 50, 0.2, False)
    assert m is not None

    updated = modify_repo.update_measurement(
        str(m.id),
        pm10=99.0,
        pm25=None,  # ignored
        dust=123.0,
        is_calima=True,
    )

    assert updated.data.pm10 == 99.0
    assert updated.data.pm25 == 5.0  # unchanged
    assert updated.data.dust == 123.0
    assert updated.data.is_calima is True


def test_add_calima_event_unknown_location_returns_none(modify_repo: ModifyAirRepository):
    e = modify_repo.add_calima_event("unknown", dt(0), dt(5), peak_pm10=100, peak_dust=500, peak_aod=1.2)
    assert e is None
    assert CalimaEvent.objects.count() == 0


def test_add_calima_event_saves(modify_repo: ModifyAirRepository, location: AirLocation):
    e = modify_repo.add_calima_event("santa_cruz", dt(0), dt(5), peak_pm10=100, peak_dust=500, peak_aod=1.2)

    assert e is not None
    assert e.id is not None
    assert e.location.id == location.id
    assert e.start_time == dt(0)
    assert e.end_time == dt(5)
    assert CalimaEvent.objects.count() == 1


# -----------------------
# ReadAirRepository tests
# -----------------------

def test_get_measurements_unknown_location_returns_empty(read_repo: ReadAirRepository):
    assert read_repo.get_measurements("unknown") == []


def test_get_measurements_returns_sorted_by_timestamp(
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    modify_repo.add_measurement("santa_cruz", dt(2), 20, 8, 120, 0.5, False)
    modify_repo.add_measurement("santa_cruz", dt(0), 10, 5, 50, 0.2, False)
    modify_repo.add_measurement("santa_cruz", dt(1), 30, 10, 200, 0.9, True)

    rows = read_repo.get_measurements("santa_cruz")
    assert [r.data.timestamp for r in rows] == [dt(0), dt(1), dt(2)]


def test_get_latest_returns_none_for_unknown_location(read_repo: ReadAirRepository):
    assert read_repo.get_latest("unknown") is None


def test_get_latest_returns_most_recent(
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    modify_repo.add_measurement("santa_cruz", dt(0), 10, 5, 50, 0.2, False)
    modify_repo.add_measurement("santa_cruz", dt(2), 20, 8, 120, 0.5, False)
    modify_repo.add_measurement("santa_cruz", dt(1), 30, 10, 200, 0.9, True)

    latest = read_repo.get_latest("santa_cruz")
    assert latest is not None
    assert latest.data.timestamp == dt(2)


def test_get_range_filters_inclusive(
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    modify_repo.add_measurement("santa_cruz", dt(0), 10, 5, 50, 0.2, False)
    modify_repo.add_measurement("santa_cruz", dt(1), 30, 10, 200, 0.9, True)
    modify_repo.add_measurement("santa_cruz", dt(2), 20, 8, 120, 0.5, False)

    rows = read_repo.get_range("santa_cruz", start=dt(1), end=dt(2))
    assert [r.data.timestamp for r in rows] == [dt(1), dt(2)]


def test_find_calima_hours_filters_true(
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    modify_repo.add_measurement("santa_cruz", dt(0), 10, 5, 50, 0.2, False)
    modify_repo.add_measurement("santa_cruz", dt(1), 30, 10, 200, 0.9, True)
    modify_repo.add_measurement("santa_cruz", dt(2), 40, 15, 250, 1.1, True)

    rows = read_repo.find_calima_hours("santa_cruz")
    assert [r.data.timestamp for r in rows] == [dt(1), dt(2)]
    assert all(r.data.is_calima for r in rows)


def test_get_calima_events_returns_newest_first(
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    modify_repo.add_calima_event("santa_cruz", dt(0), dt(3), peak_pm10=80, peak_dust=400, peak_aod=0.9)
    modify_repo.add_calima_event("santa_cruz", dt(10), dt(12), peak_pm10=120, peak_dust=600, peak_aod=1.3)

    events = read_repo.get_calima_events("santa_cruz")
    assert [e.start_time for e in events] == [dt(10), dt(0)]


def test_get_events_over_threshold_filters_and_sorts(
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    modify_repo.add_calima_event("santa_cruz", dt(0), dt(2), peak_pm10=50, peak_dust=200, peak_aod=0.5)
    modify_repo.add_calima_event("santa_cruz", dt(5), dt(6), peak_pm10=150, peak_dust=700, peak_aod=1.4)
    modify_repo.add_calima_event("santa_cruz", dt(10), dt(12), peak_pm10=110, peak_dust=500, peak_aod=1.0)

    events = read_repo.get_events_over_threshold("santa_cruz", pm10_min=100)

    # Code sorts by "-start_time", so newest (dt(10)) first.
    assert [e.start_time for e in events] == [dt(10), dt(5)]
    assert [e.peak_pm10 for e in events] == [110.0, 150.0]


# -----------------------
# Aggregation pipeline tests (monkeypatch)
# -----------------------
def _normalize_pipeline(args: tuple) -> list[dict]:
    if len(args) == 1 and isinstance(args[0], list):
        return args[0]
    return list(args)


def test_get_daily_avg_builds_expected_pipeline(
    monkeypatch,
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    captured: dict[str, list[dict]] = {}

    def fake_aggregate(self, *args, **kwargs):
        pipe = _normalize_pipeline(args)
        captured["pipeline"] = pipe
        return [{"_id": "2026-01-01", "pm10": 20.0, "pm25": 10.0, "dust": 100.0, "aod": 0.5}]


    monkeypatch.setattr(AirMeasurement.objects.__class__, "aggregate", fake_aggregate, raising=False)

    out = read_repo.get_daily_avg("santa_cruz")

    assert out and out[0]["_id"] == "2026-01-01"
    pipe = captured["pipeline"]
    assert pipe[0] == {"$match": {"location": location.id}}
    assert "$group" in pipe[1]
    assert pipe[-1] == {"$sort": {"_id": 1}}


def test_get_daily_max_builds_expected_pipeline(
    monkeypatch,
    modify_repo: ModifyAirRepository,
    read_repo: ReadAirRepository,
    location: AirLocation,
):
    captured: dict[str, list[dict]] = {}

    def fake_aggregate(self, *args, **kwargs):
        pipe = _normalize_pipeline(args)
        captured["pipeline"] = pipe
        return [{"_id": "2026-01-01", "pm10": 40.0, "pm25": 15.0, "dust": 250.0, "aod": 1.1}]

    monkeypatch.setattr(AirMeasurement.objects.__class__, "aggregate", fake_aggregate, raising=False)

    out = read_repo.get_daily_max("santa_cruz")

    assert out and out[0]["pm10"] == 40.0
    pipe = captured["pipeline"]
    assert pipe[0] == {"$match": {"location": location.id}}
    assert "$group" in pipe[1]
    assert pipe[-1] == {"$sort": {"_id": 1}}


import pytest
from mongoengine import DoesNotExist

from src.repository.repository import ModifyAirRepository, ReadAirRepository
from src.repository.model import AirLocation, AirMeasurement, CalimaEvent


# -------------------------
# Explicit DoesNotExist tests
# -------------------------

def test_add_measurement_handles_doesnotexist(monkeypatch, modify_repo: ModifyAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    m = modify_repo.add_measurement(
        location_name="santa_cruz",
        timestamp=dt(0),
        pm10=10.0,
        pm25=5.0,
        dust=1.0,
        aod=0.1,
        is_calima=False,
    )
    assert m is None


def test_bulk_add_measurements_handles_doesnotexist(monkeypatch, modify_repo: ModifyAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    inserted = modify_repo.bulk_add_measurements("santa_cruz", measurements=[])
    assert inserted == 0


def test_delete_measurements_for_location_handles_doesnotexist(monkeypatch, modify_repo: ModifyAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    deleted = modify_repo.delete_measurements_for_location("santa_cruz")
    assert deleted == 0


def test_add_calima_event_handles_doesnotexist(monkeypatch, modify_repo: ModifyAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    e = modify_repo.add_calima_event(
        location_name="santa_cruz",
        start=dt(0),
        end=dt(2),
        peak_pm10=100,
        peak_dust=500,
        peak_aod=1.2,
    )
    assert e is None


def test_read_get_measurements_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    rows = read_repo.get_measurements("santa_cruz")
    assert rows == []


def test_read_get_latest_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    latest = read_repo.get_latest("santa_cruz")
    assert latest is None


def test_read_get_range_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    rows = read_repo.get_range("santa_cruz", start=dt(0), end=dt(2))
    assert rows == []


def test_read_find_calima_hours_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    rows = read_repo.find_calima_hours("santa_cruz")
    assert rows == []


def test_read_get_daily_avg_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    out = read_repo.get_daily_avg("santa_cruz")
    assert out == []


def test_read_get_daily_max_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    out = read_repo.get_daily_max("santa_cruz")
    assert out == []


def test_read_get_calima_events_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    events = read_repo.get_calima_events("santa_cruz")
    assert events == []


def test_read_get_events_over_threshold_handles_doesnotexist(monkeypatch, read_repo: ReadAirRepository):
    def boom(*args, **kwargs):
        raise DoesNotExist()

    monkeypatch.setattr(AirLocation.objects, "get", boom, raising=False)

    events = read_repo.get_events_over_threshold("santa_cruz", pm10_min=100)
    assert events == []
