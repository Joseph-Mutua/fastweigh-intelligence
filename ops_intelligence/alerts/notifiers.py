from __future__ import annotations

import json
import smtplib
from datetime import datetime
from email.message import EmailMessage

import httpx

from ops_intelligence.alerts.rules import AlertFinding
from ops_intelligence.config import TenantConfig, env_or_empty


def notify_email(cfg: TenantConfig, findings: list[AlertFinding]) -> bool:
    if not cfg.email.enabled or not findings:
        return False

    username = env_or_empty(cfg.email.username_env)
    password = env_or_empty(cfg.email.password_env)
    if not username or not password:
        raise ValueError("Alert email enabled but SMTP credentials are missing")

    lines = [f"{f.severity.upper()} | {f.name} | {f.details}" for f in findings]

    msg = EmailMessage()
    msg["Subject"] = f"Fast-Weigh KPI Alerts - {cfg.tenant_name}"
    msg["From"] = cfg.email.sender
    msg["To"] = ", ".join(cfg.email.recipients)
    msg.set_content("\n".join(lines))

    with smtplib.SMTP(cfg.email.smtp_host, cfg.email.smtp_port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(msg)
    return True


def notify_webhook(cfg: TenantConfig, findings: list[AlertFinding]) -> bool:
    if not cfg.webhook.enabled or not findings:
        return False

    headers = {"Content-Type": "application/json"}
    if cfg.webhook.bearer_token_env:
        token = env_or_empty(cfg.webhook.bearer_token_env)
        if token:
            headers["Authorization"] = f"Bearer {token}"

    payload = {
        "tenant": cfg.tenant_name,
        "triggeredAt": datetime.utcnow().isoformat() + "Z",
        "alerts": [
            {
                "name": f.name,
                "severity": f.severity,
                "details": f.details,
            }
            for f in findings
        ],
    }

    response = httpx.post(cfg.webhook.url, headers=headers, content=json.dumps(payload), timeout=30)
    response.raise_for_status()
    return True
