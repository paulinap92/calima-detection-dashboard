"""
Calima event detection.

This module contains the `CalimaDetector` class, responsible for detecting
Saharan dust (calima) episodes from stored hourly air-quality measurements.

A "calima event" is defined as a continuous block of hours classified as calima.
Only CLOSED events (i.e., those that have a confirmed ending hour) are persisted.

The detector is designed to be idempotent:
- It never overwrites existing events.
- It starts scanning only after the end timestamp of the newest stored event
  to avoid creating duplicates.
"""

from datetime import datetime
from src.repository.repository import ReadAirRepository, ModifyAirRepository
from src.repository.model import AirMeasurement, CalimaEvent


class CalimaDetector:
    """
    Detect Saharan dust (calima) episodes based on stored hourly measurements.

    Rules:
        1. Each hour is classified as calima or not using threshold rules.
        2. A calima event is a continuous block of calima hours lasting >= 3 hours.
        3. Only CLOSED events (event with a confirmed end) are saved.
        4. The detector never overwrites already stored events.
        5. Detection starts AFTER the last saved event to avoid duplicates.

    Dependencies:
        read_repo: Repository used to read measurements and existing events.
        modify_repo: Repository used to persist newly detected events.
    """

    def __init__(self, read_repo: ReadAirRepository, modify_repo: ModifyAirRepository):
        """
        Initialize the detector.

        Args:
            read_repo: Read repository implementation.
            modify_repo: Modify repository implementation.
        """
        self.read_repo = read_repo
        self.modify_repo = modify_repo

    # ---------------------------------------------------
    # Hour-level heuristic
    # ---------------------------------------------------
    def is_calima_from_values(
        self,
        pm10: float | None,
        pm25: float | None,
        dust: float | None,
        aod: float | None
    ) -> bool:
        """
        Classify a single hour as calima based on measurement values.

        Calima conditions (heuristic):
            - dust > 150, OR
            - pm10 > 50 AND aod > 0.5, OR
            - pm25 > 35 AND pm10 > 60

        Args:
            pm10: PM10 concentration for the hour.
            pm25: PM2.5 concentration for the hour.
            dust: Dust concentration for the hour.
            aod: Aerosol optical depth for the hour.

        Returns:
            True if the hour matches calima conditions, otherwise False.
        """
        # dust very high
        if dust is not None and dust > 150:
            return True

        # PM10 + AOD signature
        if pm10 is not None and pm10 > 50:
            if aod is not None and aod > 0.5:
                return True

        # PM25 + PM10 in stronger episodes
        if pm25 is not None and pm25 > 35:
            if pm10 is not None and pm10 > 60:
                return True

        return False

    # ---------------------------------------------------
    # Hour-level classification from DB document
    # ---------------------------------------------------
    def is_hour_calima(self, m: AirMeasurement) -> bool:
        """
        Classify a stored measurement hour as calima.

        Args:
            m: AirMeasurement document (expected to have `.data` with values).

        Returns:
            True if the measurement hour is calima, otherwise False.
        """
        d = m.data
        return self.is_calima_from_values(
            pm10=d.pm10,
            pm25=d.pm25,
            dust=d.dust,
            aod=d.aod
        )

    # ---------------------------------------------------
    # Main event detection
    # ---------------------------------------------------
    def detect_events(self, location: str) -> list[CalimaEvent]:
        """
        Detect and persist new CLOSED calima events for the given location.

        Detection flow:
            1. Load existing events for the location.
            2. Determine the end timestamp of the newest saved event (if any).
            3. Load measurements AFTER that timestamp (or all measurements if none).
            4. Scan for continuous calima-hour sequences.
            5. Persist only sequences that are CLOSED and last >= 3 hours.

        Important:
            - This method does NOT persist an event that is still "open" at the
              end of the available measurement list (no confirmed ending hour).
            - Duration is computed as the number of hourly samples in the run.
              For example, timestamps 00:00, 01:00, 02:00 represent 3 hours.

        Args:
            location: Location identifier (must match repository storage keys).

        Returns:
            List of newly created CalimaEvent objects (only those persisted).
        """
        # ----------------------------------------------
        # 1. Read newest stored event
        # ----------------------------------------------
        old_events = self.read_repo.get_calima_events(location)
        last_end_ts = None

        if old_events:
            # events ordered by -start_time → first is newest
            last_end_ts = old_events[0].end_time

        # ----------------------------------------------
        # 2. Load measurements for analysis
        # ----------------------------------------------
        if last_end_ts is None:
            # first run: analyze all measurements
            measurements = self.read_repo.get_measurements(location)
        else:
            # analyze only measurements after the last known end
            measurements = self.read_repo.get_range(location, last_end_ts, datetime.max)

        if not measurements:
            return []

        # ----------------------------------------------
        # 3. Hour classification flags
        # ----------------------------------------------
        flags = [self.is_hour_calima(m) for m in measurements]

        events: list[CalimaEvent] = []

        current_start = None
        peak_pm10 = peak_dust = peak_aod = 0.0

        # Track how many consecutive calima hours we have in the current run.
        run_len = 0

        # ----------------------------------------------
        # 4. Scan continuous True sequences
        # ----------------------------------------------
        for i, (m, is_k) in enumerate(zip(measurements, flags)):

            if is_k:
                # Start a new run or continue
                if current_start is None:
                    current_start = m.data.timestamp
                    run_len = 0

                run_len += 1

                # Update peaks
                peak_pm10 = max(peak_pm10, m.data.pm10 or 0)
                peak_dust = max(peak_dust, m.data.dust or 0)
                peak_aod = max(peak_aod, m.data.aod or 0)

            else:
                # End of run → persist only a CLOSED event
                if current_start is not None:
                    start = current_start
                    end = measurements[i - 1].data.timestamp

                    # FIX: run_len counts hourly samples, so 3 consecutive hours => run_len == 3.
                    if run_len >= 3:
                        event = self.modify_repo.add_calima_event(
                            location_name=location,
                            start=start,
                            end=end,
                            peak_pm10=peak_pm10,
                            peak_dust=peak_dust,
                            peak_aod=peak_aod
                        )
                        if event:
                            events.append(event)

                # reset
                current_start = None
                peak_pm10 = peak_dust = peak_aod = 0.0
                run_len = 0

        # ----------------------------------------------
        # 5. Do not persist an "open" event at the end
        # ----------------------------------------------
        # If the last hours are calima, the run never ends with a False hour.
        # We do not save such an event because its end is not confirmed.
        return events
