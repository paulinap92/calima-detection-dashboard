import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

from src.repository.db_config import connect_nosql_db, disconnect_nosql_db
from src.service.update_service import UpdateService
from src.repository.repository import ModifyAirRepository, ReadAirRepository
from src.repository.model import AirLocation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOCS = [
    # Tenerife (multi-point)
    "santa_cruz",
    "puerto_de_la_cruz",
    "adeje",
    "puertito_de_guimar",

    # One per remaining islands
    "gran_canaria_las_palmas",
    "lanzarote_arrecife",
    "fuerteventura_puerto_del_rosario",
    "la_palma_santa_cruz",
    "la_gomera_san_sebastian",
    "el_hierro_valverde",
]


# ---------------------------------------------------------
# Ensure mandatory locations
# ---------------------------------------------------------
def ensure_locations(repo: ModifyAirRepository) -> None:
    coords: dict[str, tuple[float, float]] = {
        # Tenerife
        "santa_cruz": (28.4636, -16.2518),
        "puerto_de_la_cruz": (28.4140, -16.5449),
        "adeje": (28.1227, -16.7260),
        "puertito_de_guimar": (28.3090, -16.3810),

        # Gran Canaria
        "gran_canaria_las_palmas": (28.1235, -15.4363),

        # Lanzarote
        "lanzarote_arrecife": (28.9630, -13.5477),

        # Fuerteventura
        "fuerteventura_puerto_del_rosario": (28.5004, -13.8627),

        # La Palma
        "la_palma_santa_cruz": (28.6835, -17.7642),

        # La Gomera
        "la_gomera_san_sebastian": (28.0916, -17.1110),

        # El Hierro
        "el_hierro_valverde": (27.8069, -17.9157),
    }

    logger.info("[INIT] Checking required locations...")
    for name, (lat, lon) in coords.items():
        if AirLocation.objects(name=name).first() is None:
            repo.add_location(name, lat, lon)
            logger.info(f"[INIT] Added location: {name}")
        else:
            logger.info(f"[INIT] Location OK: {name}")

# ---------------------------------------------------------
# Import 90-day history IF database is empty
# ---------------------------------------------------------
def import_initial_history(updater: UpdateService, read_repo: ReadAirRepository, location: str) -> None:
    """
    Runs ONLY if a location has no measurements at all.
    Prevents downloading large amounts of data every scheduler cycle.
    """
    count = len(read_repo.get_measurements(location))
    if count > 0:
        logger.info(f"[HISTORY] {location}: existing data found → skipping backfill")
        return

    logger.info(f"[HISTORY] {location}: no data found → importing 30 days")
    updater.fetch_history_last_days(location, 90)
    logger.info(f"[HISTORY] {location}: initial backfill completed")


# ---------------------------------------------------------
# FULL UPDATE CYCLE (PROD ONLY)
# ---------------------------------------------------------
def run_full_update() -> None:
    try:
        connect_nosql_db()
        logger.info("=== MongoDB connected (PROD) ===")

        updater = UpdateService()
        read_repo = updater.read_repo

        # 1) ensure locations
        ensure_locations(updater.modify_repo)

        # 2) ensure historical backfill ONCE (per location)
        for loc in LOCS:
            import_initial_history(updater, read_repo, loc)

        # 3) hourly update
        logger.info("=== HOURLY UPDATE (PROD) ===")
        for loc in LOCS:
            updater.update_location(loc)

        logger.info("=== UPDATE CYCLE FINISHED (PROD) ===")

    except Exception as ex:
        logger.error(f"[ERROR] Scheduler failed: {ex}")

    finally:
        disconnect_nosql_db()
        logger.info("MongoDB disconnected (PROD)")


# ---------------------------------------------------------
# APScheduler entrypoint
# ---------------------------------------------------------
def main() -> None:
    logger.info("=== CALIMA SCHEDULER STARTED (PROD) ===")

    scheduler = BlockingScheduler()

    # Fire once immediately, then every hour
    scheduler.add_job(
        run_full_update,
        "interval",
        hours=1,
        next_run_time=datetime.now(),
        id="prod_update",
        replace_existing=True,

    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped manually.")
    finally:
        # safe cleanup in case the scheduler is interrupted mid-run
        disconnect_nosql_db()


if __name__ == "__main__":
    main()
