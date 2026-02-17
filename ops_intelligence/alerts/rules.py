from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import duckdb

from ops_intelligence.config import AlertThresholdConfig


@dataclass
class AlertFinding:
    name: str
    severity: str
    details: str


def _iter_rows(conn: duckdb.DuckDBPyConnection, sql: str) -> Iterable[tuple]:
    return conn.execute(sql).fetchall()


def evaluate_alerts(
    conn: duckdb.DuckDBPyConnection,
    thresholds: AlertThresholdConfig,
) -> list[AlertFinding]:
    findings: list[AlertFinding] = []

    yard_rows = _iter_rows(
        conn,
        f"""
        SELECT service_date, location_id, avg_time_in_yard_minutes
        FROM gold_plant_ops_daily
        WHERE avg_time_in_yard_minutes > {thresholds.yard_time_minutes}
        ORDER BY service_date DESC
        LIMIT 20
        """,
    )
    for service_date, location_id, value in yard_rows:
        findings.append(
            AlertFinding(
                name="yard_congestion",
                severity="high",
                details=f"{service_date} location={location_id} avg_time_in_yard={value:.1f}m",
            )
        )

    variance_rows = _iter_rows(
        conn,
        f"""
        SELECT service_date, location_id, avg_load_variance_pct
        FROM gold_plant_ops_daily
        WHERE avg_load_variance_pct > {thresholds.load_variance_percent}
        ORDER BY service_date DESC
        LIMIT 20
        """,
    )
    for service_date, location_id, value in variance_rows:
        findings.append(
            AlertFinding(
                name="load_variance",
                severity="medium",
                details=f"{service_date} location={location_id} avg_load_variance_pct={value:.2f}",
            )
        )

    dispatch_rows = _iter_rows(
        conn,
        f"""
        SELECT service_date, location_id, avg_delivery_minutes
        FROM gold_dispatch_daily
        WHERE avg_delivery_minutes > {thresholds.late_delivery_minutes}
        ORDER BY service_date DESC
        LIMIT 20
        """,
    )
    for service_date, location_id, value in dispatch_rows:
        findings.append(
            AlertFinding(
                name="late_deliveries",
                severity="high",
                details=f"{service_date} location={location_id} avg_delivery_minutes={value:.1f}",
            )
        )

    ar_rows = _iter_rows(
        conn,
        f"""
        SELECT as_of_date, ar_90_plus
        FROM gold_billing_ar_daily
        WHERE ar_90_plus > {thresholds.ar_overdue_amount}
        ORDER BY as_of_date DESC
        LIMIT 5
        """,
    )
    for as_of_date, value in ar_rows:
        findings.append(
            AlertFinding(
                name="ar_aging_risk",
                severity="high",
                details=f"{as_of_date} ar_90_plus={value:.2f}",
            )
        )

    return findings
