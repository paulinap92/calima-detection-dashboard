from __future__ import annotations

from datetime import datetime
import types

import pytest
import requests

import src.api.open_meteo_api as aq


# -----------------------------
# Helpers: fake requests.get
# -----------------------------
class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _sample_hourly_payload(times, pm10, pm25, dust, aod):
    return {
        "hourly": {
            "time": times,
            "pm10": pm10,
            "pm2_5": pm25,
            "dust": dust,
            "aerosol_optical_depth": aod,
        }
    }


# -----------------------------
# Unit tests: helpers
# -----------------------------
def test__to_datetime_list_converts_to_naive():
    times = ["2026-01-01T00:00", "2026-01-01T01:00"]
    out = aq._to_datetime_list(times)

    assert out == [datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 1, 1, 0)]
    assert all(t.tzinfo is None for t in out)


def test__nearest_index_returns_closest():
    times = [
        datetime(2026, 1, 1, 0, 0),
        datetime(2026, 1, 1, 1, 0),
        datetime(2026, 1, 1, 2, 0),
    ]
    target = datetime(2026, 1, 1, 1, 30)
    # closest is 1:00 or 2:00; distance equal (1800s each) -> min picks first => index 1
    assert aq._nearest_index(times, target) == 1


# -----------------------------
# fetch_history_days: validation
# -----------------------------
def test_fetch_history_days_unknown_location_raises():
    with pytest.raises(ValueError, match="Unknown location"):
        aq.fetch_history_days("unknown_city", 5)


def test_fetch_history_days_days_over_90_raises():
    with pytest.raises(ValueError, match="past_days up to 90"):
        aq.fetch_history_days("santa_cruz", 91)


# -----------------------------
# fetch_history_days: requests
# -----------------------------
def test_fetch_history_days_calls_requests_with_expected_params(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout

        payload = _sample_hourly_payload(
            times=["2026-01-01T00:00"],
            pm10=[10.0],
            pm25=[5.0],
            dust=[100.0],
            aod=[0.2],
        )
        return _FakeResponse(payload, 200)

    monkeypatch.setattr(aq.requests, "get", fake_get)

    out = aq.fetch_history_days("santa_cruz", 7)

    assert captured["url"] == aq.FORECAST_URL
    assert captured["timeout"] == 15
    assert captured["params"]["past_days"] == 7
    assert captured["params"]["forecast_days"] == 0
    assert captured["params"]["timezone"] == "UTC"
    assert captured["params"]["hourly"] == "pm10,pm2_5,dust,aerosol_optical_depth"

    # data mapping
    assert out.time == [datetime(2026, 1, 1, 0, 0)]
    assert out.pm10 == [10.0]
    assert out.pm25 == [5.0]
    assert out.dust == [100.0]
    assert out.aod == [0.2]


def test_fetch_history_days_raises_for_http_error(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        return _FakeResponse({}, 500)

    monkeypatch.setattr(aq.requests, "get", fake_get)

    with pytest.raises(requests.HTTPError):
        aq.fetch_history_days("santa_cruz", 1)


# -----------------------------
# fetch_update: validation + requests
# -----------------------------
def test_fetch_update_unknown_location_raises():
    with pytest.raises(ValueError, match="Unknown location"):
        aq.fetch_update("unknown_city")


def test_fetch_update_calls_requests_with_expected_params(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout

        payload = _sample_hourly_payload(
            times=["2026-01-10T12:00", "2026-01-10T13:00"],
            pm10=[20.0, 21.0],
            pm25=[7.0, 7.5],
            dust=[50.0, 55.0],
            aod=[0.1, 0.15],
        )
        return _FakeResponse(payload, 200)

    monkeypatch.setattr(aq.requests, "get", fake_get)

    out = aq.fetch_update("adeje")

    assert captured["url"] == aq.FORECAST_URL
    assert captured["timeout"] == 15
    assert captured["params"]["past_days"] == 2
    assert captured["params"]["forecast_days"] == 3
    assert out.pm10 == [20.0, 21.0]
    assert out.pm25 == [7.0, 7.5]
    assert out.dust == [50.0, 55.0]
    assert out.aod == [0.1, 0.15]


# -----------------------------
# get_current_point: logic (no HTTP)
# -----------------------------
def test_get_current_point_picks_nearest_and_sets_calima_true_by_dust(monkeypatch):
    # Arrange: prepare AirQualityData with known times
    times = [
        datetime(2026, 1, 10, 10, 0),
        datetime(2026, 1, 10, 11, 0),
        datetime(2026, 1, 10, 12, 0),
    ]
    fake_aq = aq.AirQualityData(
        time=times,
        pm10=[10.0, 10.0, 10.0],
        pm25=[5.0, 5.0, 5.0],
        dust=[100.0, 151.0, 100.0],  # calima by dust on index 1
        aod=[0.1, 0.1, 0.1],
    )

    monkeypatch.setattr(aq, "fetch_update", lambda location: fake_aq)

    # Freeze "now" to 11:05 -> nearest should be 11:00 (index 1)
    class _FakeDateTime(datetime):
        @classmethod
        def utcnow(cls):
            return datetime(2026, 1, 10, 11, 5)

    monkeypatch.setattr(aq, "datetime", _FakeDateTime)

    # Act
    cp = aq.get_current_point("santa_cruz")

    # Assert
    assert cp.location == "santa_cruz"
    assert cp.time == datetime(2026, 1, 10, 11, 0)
    assert cp.dust == 151.0
    assert cp.is_calima is True


def test_get_current_point_sets_calima_true_by_pm10_and_aod(monkeypatch):
    times = [
        datetime(2026, 1, 10, 10, 0),
        datetime(2026, 1, 10, 11, 0),
    ]
    fake_aq = aq.AirQualityData(
        time=times,
        pm10=[49.0, 51.0],
        pm25=[5.0, 5.0],
        dust=[10.0, 10.0],
        aod=[0.49, 0.51],  # threshold aod > 0.5
    )
    monkeypatch.setattr(aq, "fetch_update", lambda location: fake_aq)

    class _FakeDateTime(datetime):
        @classmethod
        def utcnow(cls):
            return datetime(2026, 1, 10, 11, 10)

    monkeypatch.setattr(aq, "datetime", _FakeDateTime)

    cp = aq.get_current_point("adeje")

    assert cp.time == datetime(2026, 1, 10, 11, 0)
    assert cp.pm10 == 51.0
    assert cp.aod == 0.51
    assert cp.is_calima is True


def test_get_current_point_calima_false_when_missing_required_values(monkeypatch):
    times = [datetime(2026, 1, 10, 11, 0)]
    fake_aq = aq.AirQualityData(
        time=times,
        pm10=[60.0],
        pm25=[5.0],
        dust=[None],   # no dust
        aod=[None],    # no aod -> pm10 rule cannot trigger
    )
    monkeypatch.setattr(aq, "fetch_update", lambda location: fake_aq)

    class _FakeDateTime(datetime):
        @classmethod
        def utcnow(cls):
            return datetime(2026, 1, 10, 11, 1)

    monkeypatch.setattr(aq, "datetime", _FakeDateTime)

    cp = aq.get_current_point("puerto_de_la_cruz")
    assert cp.is_calima is False
