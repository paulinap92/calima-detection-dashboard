# src/repository/update_service.py
import logging
from datetime import datetime, timedelta

from src.api.open_meteo_api import fetch_history_days, fetch_update
from src.repository.repository import ModifyAirRepository, ReadAirRepository
from src.repository.calima_detector import CalimaDetector
from src.repository.model import AirQualityData

logger = logging.getLogger(__name__)


class UpdateService:
    """Fetch, repair, and persist hourly air-quality measurements."""

    def __init__(self):
        self.modify_repo = ModifyAirRepository()
        self.read_repo = ReadAirRepository()
        self.detector = CalimaDetector(self.read_repo, self.modify_repo)

    @staticmethod
    def _completed_hour_cutoff(now: datetime) -> datetime:
        """Return the start of the current hour.

        Only timestamps strictly before this value are treated as completed
        observations. This avoids storing the current, still-changing hour.
        """
        return now.replace(minute=0, second=0, microsecond=0)

    @staticmethod
    def _has_any_value(
        pm10: float | None,
        pm25: float | None,
        dust: float | None,
        aod: float | None,
    ) -> bool:
        return any(value is not None for value in (pm10, pm25, dust, aod))

    def _build_real_measurements(
        self,
        aq,
        *,
        cutoff: datetime,
    ) -> list[AirQualityData]:
        measurements: list[AirQualityData] = []

        for i, timestamp in enumerate(aq.time):
            timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
            if timestamp >= cutoff:
                continue

            pm10 = aq.pm10[i]
            pm25 = aq.pm25[i]
            dust = aq.dust[i]
            aod = aq.aod[i]

            # Open-Meteo can temporarily return an hour with all values null.
            # Do not persist such a placeholder; retry it during the next cycle.
            if not self._has_any_value(pm10, pm25, dust, aod):
                continue

            measurements.append(
                AirQualityData(
                    timestamp=timestamp,
                    pm10=pm10,
                    pm25=pm25,
                    dust=dust,
                    aod=aod,
                    is_calima=self.detector.is_calima_from_values(
                        pm10,
                        pm25,
                        dust,
                        aod,
                    ),
                )
            )

        return measurements

    def fetch_history_last_days(self, location: str, days: int) -> int:
        """Upsert historical data and repair missing or incomplete hours."""
        if days > 90:
            raise ValueError("Open-Meteo supports past_days <= 90.")

        aq = fetch_history_days(location, days)
        cutoff = self._completed_hour_cutoff(datetime.utcnow())
        measurements = self._build_real_measurements(aq, cutoff=cutoff)

        written = self.modify_repo.bulk_add_measurements(
            location,
            measurements,
        )
        logger.info(
            "[HISTORY] %s: inserted or repaired %s records",
            location,
            written,
        )
        return written

    def fetch_latest_update(self, location: str):
        """Persist completed real hours and return future forecast hours."""
        aq = fetch_update(location)
        now = datetime.utcnow()
        cutoff = self._completed_hour_cutoff(now)

        real_measurements = self._build_real_measurements(aq, cutoff=cutoff)
        forecast: list[AirQualityData] = []

        for i, timestamp in enumerate(aq.time):
            timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
            if timestamp < cutoff:
                continue

            pm10 = aq.pm10[i]
            pm25 = aq.pm25[i]
            dust = aq.dust[i]
            aod = aq.aod[i]

            if not self._has_any_value(pm10, pm25, dust, aod):
                continue

            forecast.append(
                AirQualityData(
                    timestamp=timestamp,
                    pm10=pm10,
                    pm25=pm25,
                    dust=dust,
                    aod=aod,
                    is_calima=self.detector.is_calima_from_values(
                        pm10,
                        pm25,
                        dust,
                        aod,
                    ),
                )
            )

        written = self.modify_repo.bulk_add_measurements(
            location,
            real_measurements,
        )
        logger.info(
            "[UPDATE] %s: inserted or repaired %s completed real hours",
            location,
            written,
        )
        return written, forecast

    def update_location(self, location: str):
        written, forecast = self.fetch_latest_update(location)
        logger.info(
            "[UPDATE] %s: wrote %s real records",
            location,
            written,
        )

        events = self.detector.detect_events(location)
        logger.info("[CALIMA] %s: %s events detected", location, len(events))
        return forecast
