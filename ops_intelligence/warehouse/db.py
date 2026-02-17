from __future__ import annotations

from pathlib import Path

import duckdb


def connect_warehouse(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute("PRAGMA enable_object_cache")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            entity TEXT PRIMARY KEY,
            last_synced_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze_events (
            entity TEXT,
            pulled_at TIMESTAMP,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            record_json TEXT,
            record_updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_events (
            alert_name TEXT,
            severity TEXT,
            details TEXT,
            triggered_at TIMESTAMP
        )
        """
    )
    return conn
