"""
MongoDB (NoSQL) connection utilities for Calima project.

This module provides helper functions to manage a MongoDB connection
using MongoEngine in a Docker-based environment.

Key assumptions:
- MongoDB runs without authentication.
- Connection is performed via a full Mongo URI.
- Environment variables are loaded from a `.env` file.
- No localhost or development fallbacks are allowed.

This module is intended to be used by:
- schedulers
- update services
- CLI scripts
- Streamlit applications
"""

import os
import logging

from mongoengine import connect, disconnect
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def connect_nosql_db() -> None:
    """
    Establish a MongoDB connection (Docker-only, no authentication).

    The connection parameters are read from environment variables.

    Required environment variables:
        - MONGO_URI: Full MongoDB connection URI
          (e.g. mongodb://mongo:27017)

    Optional environment variables:
        - MONGO_DB_NAME: Database name (default: "calima")

    Notes:
        - No localhost fallback is provided.
        - Authentication is not supported in this configuration.
        - Intended for Dockerized environments only.
        - Uses MongoEngine as the ODM layer.

    Raises:
        mongoengine.connection.MongoEngineConnectionError:
            If the connection cannot be established within the timeout.
    """
    db_name = os.getenv("MONGO_DB_NAME", "calima")
    mongo_uri = os.getenv("MONGO_URI")

    logger.info(f"[DB] Connecting (NO AUTH) db={db_name} uri={mongo_uri}")

    connect(
        db=db_name,
        host=mongo_uri,
        uuidRepresentation="standard",
        serverSelectionTimeoutMS=5000,
    )

    logger.info("[DB] Connected")


def disconnect_nosql_db() -> None:
    """
    Close the active MongoDB connection.

    This function should be called during graceful shutdown of:
    - schedulers
    - background jobs
    - long-running applications

    It disconnects the default MongoEngine connection.
    """
    disconnect()
    logger.info("[DB] Disconnected")
