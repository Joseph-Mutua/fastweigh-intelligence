from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from ops_intelligence.config import EntityConfig, TenantConfig
from ops_intelligence.graphql.client import GraphQLClient
from ops_intelligence.graphql.queries import load_query


def _nested_value(payload: dict[str, Any], dotted: str) -> Any:
    value: Any = payload
    for key in dotted.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
        if value is None:
            return None
    return value


def _as_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _read_last_synced(conn: duckdb.DuckDBPyConnection, entity: str) -> datetime | None:
    row = conn.execute("SELECT last_synced_at FROM sync_state WHERE entity = ?", [entity]).fetchone()
    if not row or row[0] is None:
        return None
    val = row[0]
    if isinstance(val, datetime):
        return val
    return None


def _upsert_last_synced(conn: duckdb.DuckDBPyConnection, entity: str, ts: datetime) -> None:
    conn.execute(
        """
        INSERT INTO sync_state(entity, last_synced_at)
        VALUES (?, ?)
        ON CONFLICT(entity) DO UPDATE SET last_synced_at=excluded.last_synced_at
        """,
        [entity, ts],
    )


def _window_range(
    conn: duckdb.DuckDBPyConnection,
    cfg: TenantConfig,
    entity: str,
    start_at: datetime | None,
    end_at: datetime,
) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cursor = start_at or _read_last_synced(conn, entity) or (end_at - timedelta(days=cfg.sync_window_days))
    while cursor < end_at:
        next_cursor = min(cursor + timedelta(days=cfg.sync_window_days), end_at)
        windows.append((cursor, next_cursor))
        cursor = next_cursor
    return windows


def _insert_bronze_rows(
    conn: duckdb.DuckDBPyConnection,
    entity: str,
    rows: list[dict[str, Any]],
    entity_cfg: EntityConfig,
    window_start: datetime,
    window_end: datetime,
) -> int:
    now = datetime.now(tz=UTC)
    payload = []
    for row in rows:
        updated_at = _as_datetime(_nested_value(row, entity_cfg.updated_at_field))
        payload.append(
            [
                entity,
                now,
                window_start,
                window_end,
                json.dumps(row, separators=(",", ":")),
                updated_at,
            ]
        )

    if payload:
        conn.executemany(
            """
            INSERT INTO bronze_events(entity, pulled_at, window_start, window_end, record_json, record_updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def sync_entities(
    conn: duckdb.DuckDBPyConnection,
    cfg: TenantConfig,
    entities: list[str] | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
) -> dict[str, int]:
    wanted = entities or list(cfg.entities.keys())
    missing = [e for e in wanted if e not in cfg.entities]
    if missing:
        raise ValueError(f"Entities missing from config: {', '.join(missing)}")

    end_ts = end_at or datetime.now(tz=UTC)
    counts: dict[str, int] = {e: 0 for e in wanted}

    with GraphQLClient(
        endpoint=cfg.graphql_endpoint,
        api_key=cfg.api_key(),
        timeout_seconds=cfg.timeout_seconds,
    ) as client:
        for entity in wanted:
            entity_cfg = cfg.entities[entity]
            query = load_query(entity_cfg.query_file)
            for window_start, window_end in _window_range(conn, cfg, entity, start_at, end_ts):
                rows = client.fetch_all_pages(
                    query=query,
                    entity_config=entity_cfg,
                    window_start=window_start,
                    window_end=window_end,
                    page_size=cfg.page_size,
                    max_pages=cfg.max_pages,
                )
                inserted = _insert_bronze_rows(
                    conn=conn,
                    entity=entity,
                    rows=rows,
                    entity_cfg=entity_cfg,
                    window_start=window_start,
                    window_end=window_end,
                )
                counts[entity] += inserted
                _upsert_last_synced(conn, entity, window_end)
    return counts


def write_sync_manifest(path: str, counts: dict[str, int]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(counts, indent=2, sort_keys=True), encoding="utf-8")
