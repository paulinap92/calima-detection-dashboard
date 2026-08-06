from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest

from src.service.update_service import UpdateService


@dataclass
class FakeAQ:
    time: list[datetime]
    pm10: list[float | None]
    pm25: list[float | None]
    dust: list[float | None]
    aod: list[float | None]


class FakeModifyRepo:
    def __init__(self):
        self.calls: list[tuple] = []
        self.last_payload = None

    def bulk_add_measurements(self, location: str, measurements: list):
        self.calls.append((location, len(measurements)))
        self.last_payload = measurements
        return len(measurements)


class FakeDetector:
    def __init__(self):
        self.value_calls: list[tuple] = []
        self.detect_calls: list[str] = []

    def is_calima_from_values(self, pm10, pm25, dust, aod) -> bool:
        self.value_calls.append((pm10, pm25, dust, aod))
        return bool((dust or 0) > 150 or (pm10 or 0) > 60)

    def detect_events(self, location: str):
        self.detect_calls.append(location)
        return ["event"]


def make_service(monkeypatch, *, now: datetime) -> UpdateService:
    service = UpdateService()
    service.modify_repo = FakeModifyRepo()
    service.detector = FakeDetector()

    import src.service.update_service as module

    class FixedDateTime(datetime):
        @classmethod
        def utcnow(cls):
            return now

    monkeypatch.setattr(module, "datetime", FixedDateTime)
    return service


def t(base: datetime, hours: int, minutes: int = 0) -> datetime:
    return base + timedelta(hours=hours, minutes=minutes)


def test_fetch_history_rejects_more_than_90_days(monkeypatch):
    service = make_service(
        monkeypatch,
        now=datetime(2026, 1, 1, 12, 30),
    )

    with pytest.raises(ValueError, match="past_days <= 90"):
        service.fetch_history_last_days("santa_cruz", 91)


def test_history_sends_existing_hours_for_upsert_repair(monkeypatch):
    base = datetime(2026, 1, 1)
    service = make_service(monkeypatch, now=t(base, 3, 30))
    aq = FakeAQ(
        time=[t(base, 0), t(base, 1), t(base, 2), t(base, 3)],
        pm10=[10, 20, 30, 40],
        pm25=[1, 2, 3, 4],
        dust=[5, 6, 7, 8],
        aod=[0.1, 0.2, 0.3, 0.4],
    )

    import src.service.update_service as module
    monkeypatch.setattr(module, "fetch_history_days", lambda *_: aq)

    written = service.fetch_history_last_days("santa_cruz", 30)

    # Current 03:00 hour is still incomplete. Completed 00:00-02:00 hours are
    # all sent to the repository so an existing incomplete record can be fixed.
    assert written == 3
    assert [m.timestamp for m in service.modify_repo.last_payload] == [
        t(base, 0),
        t(base, 1),
        t(base, 2),
    ]


def test_history_skips_completely_empty_api_hours(monkeypatch):
    base = datetime(2026, 1, 1)
    service = make_service(monkeypatch, now=t(base, 3, 30))
    aq = FakeAQ(
        time=[t(base, 0), t(base, 1), t(base, 2)],
        pm10=[10, None, 30],
        pm25=[1, None, 3],
        dust=[5, None, 7],
        aod=[0.1, None, 0.3],
    )

    import src.service.update_service as module
    monkeypatch.setattr(module, "fetch_history_days", lambda *_: aq)

    written = service.fetch_history_last_days("santa_cruz", 30)

    assert written == 2
    assert [m.timestamp for m in service.modify_repo.last_payload] == [
        t(base, 0),
        t(base, 2),
    ]


def test_latest_persists_only_completed_hours(monkeypatch):
    base = datetime(2026, 1, 1)
    service = make_service(monkeypatch, now=t(base, 2, 45))
    aq = FakeAQ(
        time=[t(base, 0), t(base, 1), t(base, 2), t(base, 3)],
        pm10=[10, 20, 30, 40],
        pm25=[1, 2, 3, 4],
        dust=[5, 6, 7, 8],
        aod=[0.1, 0.2, 0.3, 0.4],
    )

    import src.service.update_service as module
    monkeypatch.setattr(module, "fetch_update", lambda *_: aq)

    written, forecast = service.fetch_latest_update("santa_cruz")

    assert written == 2
    assert [m.timestamp for m in service.modify_repo.last_payload] == [
        t(base, 0),
        t(base, 1),
    ]
    assert [m.timestamp for m in forecast] == [t(base, 2), t(base, 3)]


def test_latest_retries_partial_hour_after_it_is_completed(monkeypatch):
    base = datetime(2026, 1, 1)
    service = make_service(monkeypatch, now=t(base, 3, 5))
    aq = FakeAQ(
        time=[t(base, 2)],
        pm10=[50],
        pm25=[None],
        dust=[100],
        aod=[None],
    )

    import src.service.update_service as module
    monkeypatch.setattr(module, "fetch_update", lambda *_: aq)

    written, forecast = service.fetch_latest_update("santa_cruz")

    assert written == 1
    assert forecast == []
    repaired = service.modify_repo.last_payload[0]
    assert repaired.timestamp == t(base, 2)
    assert repaired.pm10 == 50
    assert repaired.pm25 is None


def test_update_location_runs_detector(monkeypatch):
    base = datetime(2026, 1, 1)
    service = make_service(monkeypatch, now=t(base, 2, 5))
    aq = FakeAQ(
        time=[t(base, 0), t(base, 2)],
        pm10=[10, 20],
        pm25=[1, 2],
        dust=[5, 6],
        aod=[0.1, 0.2],
    )

    import src.service.update_service as module
    monkeypatch.setattr(module, "fetch_update", lambda *_: aq)

    service.update_location("santa_cruz")

    assert service.detector.detect_calls == ["santa_cruz"]
