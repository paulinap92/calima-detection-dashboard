import pytest

from mongoengine import connect, disconnect
from dataclasses import dataclass
from datetime import datetime, timedelta

@pytest.fixture(autouse=True)
def mongo_test_db():
    """
    Connect to an in-memory MongoDB using mongomock.

    Autouse so every test runs with a clean isolated DB connection.
    """
    try:
        import mongomock  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "mongomock is required for tests. Install with: pip install mongomock"
        ) from e

    # Modern MongoEngine + mongomock connection
    connect(
        db="mongoenginetest",
        host="mongodb://localhost",
        alias="default",
        mongo_client_class=mongomock.MongoClient,
        uuidRepresentation="standard",
    )

    yield

    disconnect(alias="default")

