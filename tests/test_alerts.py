from __future__ import annotations

import duckdb

from ops_intelligence.alerts.rules import evaluate_alerts
from ops_intelligence.config import AlertThresholdConfig


def test_alert_rules_trigger_expected_findings() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE gold_plant_ops_daily (
            service_date DATE,
            location_id TEXT,
            avg_time_in_yard_minutes DOUBLE,
            avg_load_variance_pct DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE gold_dispatch_daily (
            service_date DATE,
            location_id TEXT,
            avg_delivery_minutes DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE gold_billing_ar_daily (
            as_of_date DATE,
            ar_90_plus DOUBLE
        )
        """
    )

    conn.execute("INSERT INTO gold_plant_ops_daily VALUES ('2026-01-01','yard-1',120,8)")
    conn.execute("INSERT INTO gold_dispatch_daily VALUES ('2026-01-01','yard-1',95)")
    conn.execute("INSERT INTO gold_billing_ar_daily VALUES ('2026-01-01',12000)")

    findings = evaluate_alerts(
        conn,
        AlertThresholdConfig(
            yard_time_minutes=75,
            load_variance_percent=5,
            late_delivery_minutes=30,
            ar_overdue_amount=10000,
        ),
    )

    names = {f.name for f in findings}
    assert "yard_congestion" in names
    assert "load_variance" in names
    assert "late_deliveries" in names
    assert "ar_aging_risk" in names
