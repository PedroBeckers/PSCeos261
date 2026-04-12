from __future__ import annotations

import duckdb
from duckdb import DuckDBPyConnection

from app.core.config import DB_PATH, ensure_directories


def get_connection() -> DuckDBPyConnection:
    ensure_directories()
    return duckdb.connect(str(DB_PATH))