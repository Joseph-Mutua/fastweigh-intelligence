from __future__ import annotations

from datetime import datetime

import duckdb

from ops_intelligence.alerts.notifiers import notify_email, notify_webhook
from ops_intelligence.alerts.rules import AlertFinding, evaluate_alerts
from ops_intelligence.config import TenantConfig


def run_alert_engine(conn: duckdb.DuckDBPyConnection, cfg: TenantConfig) -> list[AlertFinding]:
    findings = evaluate_alerts(conn, cfg.alerts)
    if findings:
        conn.executemany(
            "INSERT INTO alert_events(alert_name, severity, details, triggered_at) VALUES (?, ?, ?, ?)",
            [[f.name, f.severity, f.details, datetime.utcnow()] for f in findings],
        )

    notify_email(cfg, findings)
    notify_webhook(cfg, findings)
    return findings
