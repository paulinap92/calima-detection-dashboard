# src/repository/update_service.py
import logging
from datetime import datetime

from src.api.open_meteo_api import fetch_history_days, fetch_update
from src.repository.repository import ModifyAirRepository, ReadAirRepository
from src.repository.calima_detector import CalimaDetector
from src.repository.model import AirQualityData

logger = logging.getLogger(__name__)


class UpdateService:
    """
    UpdateService 2.0

    Key principles:
    1. MongoDB stores ONLY real, historical data (timestamp <= now UTC).
       Forecast hours are never written to DB.

    2. Every update cycle delivers:
         • Real data → filter duplicates → save
         • Forecast → returned to the dashboard (not persisted)

    3. Calima events are computed only from real measurements.

    4. Duplicate protection:
         An hour is ignored if its timestamp already exists in DB.
    """

    def __init__(self):
        self.modify_repo = ModifyAirRepository()
        self.read_repo = ReadAirRepository()
        self.detector = CalimaDetector(self.read_repo, self.modify_repo)

    # ------------------------------------------------------------------
    # 1) HISTORY IMPORT — only when location has no data yet
    # ------------------------------------------------------------------
    def fetch_history_last_days(self, location: str, days: int) -> int:
        """
        Fetch up to 30 days of historical hourly data.
        Only timestamps <= now are saved.
        Forecast does NOT appear in this API call.

        Returns:
            Number of inserted records.
        """

        if days > 90:
            raise ValueError("Open-Meteo supports past_days <= 90.")

        aq = fetch_history_days(location, days)

        latest = self.read_repo.get_latest(location)
        last_ts = latest.data.timestamp if latest else None

        now = datetime.utcnow()   # naive UTC timestamp

        to_insert = []

        for i, ts in enumerate(aq.time):

            # skip duplicate hours
            if last_ts and ts <= last_ts:
                continue

            # skip future hours (in history shouldn't happen, but safe)
            if ts > now:
                continue

            pm10 = aq.pm10[i]
            pm25 = aq.pm25[i]
            dust = aq.dust[i]
            aod = aq.aod[i]

            is_calima = self.detector.is_calima_from_values(pm10, pm25, dust, aod)

            to_insert.append(
                AirQualityData(
                    timestamp=ts,
                    pm10=pm10,
                    pm25=pm25,
                    dust=dust,
                    aod=aod,
                    is_calima=is_calima,
                )
            )

        inserted = self.modify_repo.bulk_add_measurements(location, to_insert)
        logger.info(f"[HISTORY] {location}: inserted {inserted} records")

        return inserted

    # ------------------------------------------------------------------
    # 2) LATEST UPDATE — save only real hours, return forecast
    # ------------------------------------------------------------------
    def fetch_latest_update(self, location: str):
        """
        Fetch a combined dataset:
            • past_days=2 (real)
            • forecast_days=3 (future)

        Saves ONLY real data (timestamps <= now).
        Forecast hours are returned for dashboard usage.
        """

        aq = fetch_update(location)
        now = datetime.utcnow()

        latest = self.read_repo.get_latest(location)
        last_ts = latest.data.timestamp if latest else None

        real_insert_list = []
        forecast_list = []

        for i, ts in enumerate(aq.time):

            pm10 = aq.pm10[i]
            pm25 = aq.pm25[i]
            dust = aq.dust[i]
            aod = aq.aod[i]

            is_calima = self.detector.is_calima_from_values(pm10, pm25, dust, aod)

            model = AirQualityData(
                timestamp=ts,
                pm10=pm10,
                pm25=pm25,
                dust=dust,
                aod=aod,
                is_calima=is_calima,
            )

            if ts <= now:
                # REAL DATA
                if last_ts is None or ts > last_ts:
                    real_insert_list.append(model)
            else:
                # FORECAST — do not save
                forecast_list.append(model)

        inserted = self.modify_repo.bulk_add_measurements(location, real_insert_list)
        logger.info(f"[UPDATE] {location}: inserted {inserted} real hours (forecast skipped)")

        return inserted, forecast_list

    # ------------------------------------------------------------------
    # 3) FULL UPDATE — save data + detect calima events
    # ------------------------------------------------------------------
    def update_location(self, location: str):
        """
        Full update workflow:
          1. Fetch and save real data.
          2. Return forecast hours (for dashboard).
          3. Recalculate calima events based on real data only.
        """

        inserted, forecast = self.fetch_latest_update(location)

        logger.info(f"[UPDATE] {location}: added {inserted} new real records")

        events = self.detector.detect_events(location)
        logger.info(f"[CALIMA] {location}: {len(events)} events detected")

        return forecast
