from __future__ import annotations

import json
import shutil
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

import duckdb
import httpx

from ops_intelligence.config import TenantConfig, env_or_empty

REPORT_TABLES = [
    "gold_plant_ops_daily",
    "gold_dispatch_daily",
    "gold_billing_ar_daily",
    "gold_hauler_productivity_daily",
]


def export_csv_reports(conn: duckdb.DuckDBPyConnection, output_dir: str) -> list[Path]:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    target = Path(output_dir) / "reports" / ts
    target.mkdir(parents=True, exist_ok=True)
    files: list[Path] = []

    for table in REPORT_TABLES:
        file_path = target / f"{table}.csv"
        conn.execute(f"COPY (SELECT * FROM {table}) TO '{file_path.as_posix()}' (HEADER, DELIMITER ',')")
        files.append(file_path)
    return files


def push_to_shared_drive(files: list[Path], shared_drive_path: str | None) -> list[Path]:
    if not shared_drive_path:
        return []
    target_root = Path(shared_drive_path)
    target_root.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []

    for src in files:
        dst = target_root / src.name
        shutil.copy2(src, dst)
        copied.append(dst)
    return copied


def send_email_reports(cfg: TenantConfig, files: list[Path]) -> bool:
    if not cfg.email.enabled:
        return False
    if not cfg.email.recipients:
        raise ValueError("Email reporting is enabled but no recipients configured")

    username = env_or_empty(cfg.email.username_env)
    password = env_or_empty(cfg.email.password_env)
    if not username or not password:
        raise ValueError("Email reporting enabled but SMTP credentials are missing")

    msg = EmailMessage()
    msg["Subject"] = f"Fast-Weigh KPI Report - {cfg.tenant_name}"
    msg["From"] = cfg.email.sender
    msg["To"] = ", ".join(cfg.email.recipients)
    msg.set_content("Attached: latest KPI report exports.")

    for path in files:
        data = path.read_bytes()
        msg.add_attachment(data, maintype="text", subtype="csv", filename=path.name)

    with smtplib.SMTP(cfg.email.smtp_host, cfg.email.smtp_port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(msg)
    return True


def send_webhook_report(cfg: TenantConfig, files: list[Path]) -> bool:
    if not cfg.webhook.enabled:
        return False
    if not cfg.webhook.url:
        raise ValueError("Webhook reporting is enabled but URL is missing")

    headers = {"Content-Type": "application/json"}
    if cfg.webhook.bearer_token_env:
        token = env_or_empty(cfg.webhook.bearer_token_env)
        if token:
            headers["Authorization"] = f"Bearer {token}"

    payload = {
        "tenant": cfg.tenant_name,
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "files": [str(p) for p in files],
    }
    response = httpx.post(cfg.webhook.url, headers=headers, content=json.dumps(payload), timeout=30)
    response.raise_for_status()
    return True
