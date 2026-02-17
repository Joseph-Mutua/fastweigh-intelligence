from __future__ import annotations

from datetime import UTC, datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from ops_intelligence.config import TenantConfig
from ops_intelligence.pipeline import run_full_pipeline
from ops_intelligence.warehouse.db import connect_warehouse


def start_scheduler(cfg: TenantConfig, cron: str) -> None:
    scheduler = BlockingScheduler(timezone=cfg.timezone)

    def _run_job() -> None:
        conn = connect_warehouse(cfg.warehouse_path)
        try:
            run_full_pipeline(cfg=cfg, conn=conn, end_at=datetime.now(tz=UTC))
        finally:
            conn.close()

    scheduler.add_job(_run_job, CronTrigger.from_crontab(cron), id="ops_pipeline", replace_existing=True)
    scheduler.start()
