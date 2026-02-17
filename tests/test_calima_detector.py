from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest

from src.repository.calima_detector import CalimaDetector


# -----------------------
# Test doubles (no DB)
# -----------------------

@dataclass
class FakeData:
    timestamp: datetime
    pm10: float | None = None
    pm25: float | None = None
    dust: float | None = None
    aod: float | None = None


@dataclass
class FakeMeasurement:
    data: FakeData


@dataclass
class FakeEvent:
    start_time: datetime
    end_time: datetime
    peak_pm10: float | None
    peak_dust: float | None
    peak_aod: float | None


class FakeReadRepo:
    def __init__(self):
        self._events: list[FakeEvent] = []
        self._measurements: list[FakeMeasurement] = []

        self.calls: list[tuple] = []

    def set_events(self, events: list[FakeEvent]) -> None:
        self._events = events

    def set_measurements(self, measurements: list[FakeMeasurement]) -> None:
        self._measurements = measurements

    # API expected by CalimaDetector
    def get_calima_events(self, location_name: str):
        self.calls.append(("get_calima_events", location_name))
        return self._events

    def get_measurements(self, location_name: str):
        self.calls.append(("get_measurements", location_name))
        return self._measurements

    def get_range(self, location_name: str, start: datetime, end: datetime):
        self.calls.append(("get_range", location_name, start, end))
        # detector expects inclusive start; implement typical behavior:
        return [m for m in self._measurements if start <= m.data.timestamp <= end]


class FakeModifyRepo:
    def __init__(self):
        self.added: list[FakeEvent] = []
        self.calls: list[tuple] = []

    def add_calima_event(
        self,
        location_name: str,
        start: datetime,
        end: datetime,
        peak_pm10: float | None,
        peak_dust: float | None,
        peak_aod: float | None,
    ):
        self.calls.append(("add_calima_event", location_name, start, end, peak_pm10, peak_dust, peak_aod))
        ev = FakeEvent(
            start_time=start,
            end_time=end,
            peak_pm10=peak_pm10,
            peak_dust=peak_dust,
            peak_aod=peak_aod,
        )
        self.added.append(ev)
        return ev


# -----------------------
# Helpers
# -----------------------

def dt(h: int) -> datetime:
    return datetime(2026, 1, 1, 0, 0, 0) + timedelta(hours=h)


def m(
    h: int,
    pm10: float | None = None,
    pm25: float | None = None,
    dust: float | None = None,
    aod: float | None = None,
) -> FakeMeasurement:
    return FakeMeasurement(FakeData(timestamp=dt(h), pm10=pm10, pm25=pm25, dust=dust, aod=aod))


# -----------------------
# Fixtures
# -----------------------

@pytest.fixture()
def read_repo() -> FakeReadRepo:
    return FakeReadRepo()


@pytest.fixture()
def modify_repo() -> FakeModifyRepo:
    return FakeModifyRepo()


@pytest.fixture()
def detector(read_repo: FakeReadRepo, modify_repo: FakeModifyRepo) -> CalimaDetector:
    return CalimaDetector(read_repo=read_repo, modify_repo=modify_repo)


# -----------------------
# Unit tests: heuristics
# -----------------------

@pytest.mark.parametrize(
    "pm10,pm25,dust,aod,expected",
    [
        # dust rule
        (None, None, 151, None, True),
        (None, None, 150, None, False),
        # pm10 + aod rule
        (51, None, None, 0.51, True),
        (51, None, None, 0.5, False),
        (50, None, None, 0.9, False),
        # pm25 + pm10 rule
        (61, 36, None, None, True),
        (60, 36, None, None, False),
        (61, 35, None, None, False),
        # no data -> false
        (None, None, None, None, False),
    ],
)
def test_is_calima_from_values(detector: CalimaDetector, pm10, pm25, dust, aod, expected):
    assert detector.is_calima_from_values(pm10=pm10, pm25=pm25, dust=dust, aod=aod) is expected


def test_is_hour_calima_reads_from_measurement(detector: CalimaDetector):
    meas = m(0, pm10=55, aod=0.6)
    assert detector.is_hour_calima(meas) is True


# -----------------------
# Integration-ish: detect_events
# -----------------------

def test_detect_events_returns_empty_when_no_measurements(detector: CalimaDetector, read_repo: FakeReadRepo):
    read_repo.set_events([])
    read_repo.set_measurements([])
    assert detector.detect_events("santa_cruz") == []


def test_detect_events_first_run_uses_get_measurements(detector: CalimaDetector, read_repo: FakeReadRepo):
    read_repo.set_events([])  # no old events => first run
    read_repo.set_measurements([m(0, dust=200), m(1, dust=200), m(2, dust=200), m(3, dust=10)])  # closed run (0-2)

    detector.detect_events("santa_cruz")

    assert ("get_measurements", "santa_cruz") in read_repo.calls
    assert all(call[0] != "get_range" for call in read_repo.calls)


def test_detect_events_subsequent_run_uses_get_range_after_last_end(
    detector: CalimaDetector,
    read_repo: FakeReadRepo,
):
    # old events ordered by -start_time, detector uses old_events[0].end_time
    old = FakeEvent(start_time=dt(10), end_time=dt(12), peak_pm10=0, peak_dust=0, peak_aod=0)
    read_repo.set_events([old])

    # include data before and after last_end; get_range should filter
    read_repo.set_measurements(
        [
            m(11, dust=200),  # should be ignored by get_range start=12
            m(12, dust=10),
            m(13, dust=200),
            m(14, dust=200),
            m(15, dust=200),
            m(16, dust=10),
        ]
    )

    detector.detect_events("santa_cruz")

    # verify get_range used with last_end_ts
    assert any(call[0] == "get_range" and call[1] == "santa_cruz" and call[2] == dt(12) for call in read_repo.calls)


def test_detect_events_persists_only_closed_runs_length_at_least_3(
    detector: CalimaDetector,
    read_repo: FakeReadRepo,
    modify_repo: FakeModifyRepo,
):
    read_repo.set_events([])

    # Run1: 3 hours calima (0,1,2) then False => should save event [0..2]
    # Run2: 2 hours calima (4,5) then False => should NOT save
    read_repo.set_measurements(
        [
            m(0, dust=200),
            m(1, dust=200),
            m(2, dust=200),
            m(3, dust=10),
            m(4, dust=200),
            m(5, dust=200),
            m(6, dust=10),
        ]
    )

    events = detector.detect_events("santa_cruz")

    assert len(events) == 1
    assert len(modify_repo.added) == 1
    ev = modify_repo.added[0]
    assert ev.start_time == dt(0)
    assert ev.end_time == dt(2)


def test_detect_events_does_not_persist_open_run_at_end(
    detector: CalimaDetector,
    read_repo: FakeReadRepo,
    modify_repo: FakeModifyRepo,
):
    read_repo.set_events([])

    # Calima run reaches end of list without a closing False -> MUST NOT be saved
    read_repo.set_measurements([m(0, dust=200), m(1, dust=200), m(2, dust=200)])

    events = detector.detect_events("santa_cruz")

    assert events == []
    assert modify_repo.added == []


def test_detect_events_calculates_peaks_with_none_safe_defaults(
    detector: CalimaDetector,
    read_repo: FakeReadRepo,
    modify_repo: FakeModifyRepo,
):
    read_repo.set_events([])

    # 3-hour closed run. Include None values; peaks should use 0 for None
    read_repo.set_measurements(
        [
            m(0, pm10=None, dust=160, aod=None),    # calima via dust
            m(1, pm10=80, dust=None, aod=0.7),      # calima via pm10+aod
            m(2, pm10=70, dust=300, aod=0.2),       # calima via dust
            m(3, dust=10),
        ]
    )

    detector.detect_events("santa_cruz")

    ev = modify_repo.added[0]
    assert ev.peak_pm10 == 80
    assert ev.peak_dust == 300
    assert ev.peak_aod == 0.7


def test_detect_events_multiple_closed_runs_creates_multiple_events(
    detector: CalimaDetector,
    read_repo: FakeReadRepo,
    modify_repo: FakeModifyRepo,
):
    read_repo.set_events([])

    # Two separate valid closed runs
    read_repo.set_measurements(
        [
            m(0, dust=200), m(1, dust=200), m(2, dust=200), m(3, dust=10),
            m(4, pm10=70, aod=0.8), m(5, pm10=70, aod=0.8), m(6, pm10=70, aod=0.8), m(7, pm10=10, aod=0.1),
        ]
    )

    events = detector.detect_events("santa_cruz")

    assert len(events) == 2
    assert [e.start_time for e in modify_repo.added] == [dt(0), dt(4)]
    assert [e.end_time for e in modify_repo.added] == [dt(2), dt(6)]


def test_detect_events_idempotent_wrt_last_event_end(
    detector: CalimaDetector,
    read_repo: FakeReadRepo,
    modify_repo: FakeModifyRepo,
):
    """
    Detector should start AFTER the newest stored event end, so it won't create duplicates.
    We simulate this by returning an old event whose end cuts off an earlier run.
    """
    old = FakeEvent(start_time=dt(0), end_time=dt(2), peak_pm10=0, peak_dust=0, peak_aod=0)
    read_repo.set_events([old])

    # Data contains an earlier calima run (0-2) but we should only analyze from start=2 onward.
    # From 2 onward there's only an "open" run at the end -> should not be persisted.
    read_repo.set_measurements([m(0, dust=200), m(1, dust=200), m(2, dust=200), m(3, dust=200)])

    events = detector.detect_events("santa_cruz")

    assert events == []
    assert modify_repo.added == []

def test_detect_events_does_not_append_when_add_calima_event_returns_none(
    detector: CalimaDetector,
    read_repo: FakeReadRepo,
    modify_repo: FakeModifyRepo,
    monkeypatch,
):
    read_repo.set_events([])

    # Closed run of 3 hours -> detector will TRY to persist one event
    read_repo.set_measurements(
        [m(0, dust=200), m(1, dust=200), m(2, dust=200), m(3, dust=10)]
    )

    # Force repo to "fail to save" and return None (this is what happens in your real repo
    # when location does not exist, for example)
    def return_none(*args, **kwargs):
        return None

    monkeypatch.setattr(modify_repo, "add_calima_event", return_none)

    events = detector.detect_events("santa_cruz")

    # Detector should not append None
    assert events == []
