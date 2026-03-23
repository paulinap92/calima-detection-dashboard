"""
MongoDB (NoSQL) connection utilities for Calima project.

Supports MongoDB Atlas using a full MongoDB connection URI.

Environment variables:
    - MONGO_URI
    - MONGO_DB_NAME (optional, default: calima)
"""

import os
import logging

from mongoengine import connect, disconnect
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def connect_nosql_db() -> None:
    """
    Establish a MongoDB connection using MongoDB Atlas.
    """

    db_name = os.getenv("MONGO_DB_NAME", "calima")
    mongo_uri = os.getenv("MONGO_URI")

    if not mongo_uri:
        raise ValueError("MONGO_URI is not set")

    logger.info(f"[DB] Connecting to MongoDB Atlas db={db_name}")

    connect(
        db=db_name,
        host=mongo_uri,
        uuidRepresentation="standard",
        serverSelectionTimeoutMS=5000,
    )

    logger.info("[DB] Connected")


def disconnect_nosql_db() -> None:
    """
    Close MongoDB connection.
    """
    disconnect()
    logger.info("[DB] Disconnected")