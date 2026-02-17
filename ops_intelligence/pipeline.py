from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ops_intelligence.alerts.engine import run_alert_engine
from ops_intelligence.config import TenantConfig
from ops_intelligence.extraction.sync import sync_entities, write_sync_manifest
from ops_intelligence.reports.exporter import (
    export_csv_reports,
    push_to_shared_drive,
    send_email_reports,
    send_webhook_report,
)
from ops_intelligence.warehouse.modeling import run_gold_models, run_silver_models


def run_modeling(cfg: TenantConfig, conn) -> None:  # type: ignore[no-untyped-def]
    run_silver_models(conn)
    run_gold_models(conn)


def run_reporting(cfg: TenantConfig, conn) -> dict[str, object]:  # type: ignore[no-untyped-def]
    exported = export_csv_reports(conn, cfg.output_dir)
    copied = push_to_shared_drive(exported, cfg.shared_drive_path)
    emailed = send_email_reports(cfg, exported)
    webhook_sent = send_webhook_report(cfg, exported)
    return {
        "exported": [str(p) for p in exported],
        "copied": [str(p) for p in copied],
        "emailed": emailed,
        "webhook_sent": webhook_sent,
    }


def run_full_pipeline(
    cfg: TenantConfig,
    conn,
    entities: list[str] | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
) -> dict[str, object]:  # type: ignore[no-untyped-def]
    end_ts = end_at or datetime.now(tz=UTC)
    sync_counts = sync_entities(conn=conn, cfg=cfg, entities=entities, start_at=start_at, end_at=end_ts)

    manifest_path = Path(cfg.output_dir) / "manifests" / f"sync_{end_ts.strftime('%Y%m%d_%H%M%S')}.json"
    write_sync_manifest(str(manifest_path), sync_counts)

    run_modeling(cfg, conn)
    report_status = run_reporting(cfg, conn)
    alerts = run_alert_engine(conn, cfg)

    return {
        "sync_counts": sync_counts,
        "manifest": str(manifest_path),
        "reports": report_status,
        "alerts": [a.__dict__ for a in alerts],
    }
