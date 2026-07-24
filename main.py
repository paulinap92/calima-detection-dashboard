import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from src.repository.db_config import connect_nosql_db, disconnect_nosql_db
from src.repository.model import AirLocation
from src.repository.repository import ModifyAirRepository
from src.service.update_service import UpdateService

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


def ensure_locations(repo: ModifyAirRepository) -> None:
    """Create required locations when they do not yet exist."""
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
            logger.info("[INIT] Added location: %s", name)
        else:
            logger.info("[INIT] Location OK: %s", name)


def run_full_update(*, run_backfill: bool = False) -> None:
    """Run one production update cycle.

    A 90-day historical backfill is used only during application startup.
    Hourly scheduler runs use the lighter latest-data update.
    """
    try:
        connect_nosql_db()
        logger.info("=== MongoDB connected (PROD) ===")

        updater = UpdateService()
        ensure_locations(updater.modify_repo)

        if run_backfill:
            logger.info("=== 90-DAY BACKFILL START (PROD) ===")
            for location in LOCS:
                updater.fetch_history_last_days(location, 90)
            logger.info("=== 90-DAY BACKFILL FINISHED (PROD) ===")

        logger.info("=== HOURLY UPDATE START (PROD) ===")
        for location in LOCS:
            updater.update_location(location)

        logger.info("=== UPDATE CYCLE FINISHED (PROD) ===")

    except Exception:
        logger.exception("[ERROR] Update cycle failed")

    finally:
        disconnect_nosql_db()
        logger.info("MongoDB disconnected (PROD)")


def main() -> None:
    logger.info("=== CALIMA SCHEDULER STARTED (PROD) ===")

    # Render free services may sleep. Each process start repairs gaps from
    # the previous 90 days before the normal hourly scheduler begins.
    run_full_update(run_backfill=True)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_full_update,
        "interval",
        hours=1,
        kwargs={"run_backfill": False},
        id="prod_update",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped manually.")
    finally:
        disconnect_nosql_db()


if __name__ == "__main__":
    main()
