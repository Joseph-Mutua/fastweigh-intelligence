from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError


class EmailConfig(BaseModel):
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    sender: str = ""
    recipients: list[str] = Field(default_factory=list)
    username_env: str = "SMTP_USERNAME"
    password_env: str = "SMTP_PASSWORD"


class WebhookConfig(BaseModel):
    enabled: bool = False
    url: str = ""
    bearer_token_env: str | None = None


class AlertThresholdConfig(BaseModel):
    yard_time_minutes: int = 75
    load_variance_percent: float = 5.0
    late_delivery_minutes: int = 30
    ar_overdue_amount: float = 10000.0


class EntityConfig(BaseModel):
    query_file: str
    root_path: str
    page_info_path: str
    first_variable: str = "first"
    after_variable: str = "after"
    updated_after_variable: str = "updatedAfter"
    updated_before_variable: str = "updatedBefore"
    updated_at_field: str = "lastUpdatedAt"


class TenantConfig(BaseModel):
    tenant_name: str = "sample-tenant"
    timezone: str = "America/Chicago"
    graphql_endpoint: str = "https://graphql.fast-weigh.com/"
    api_key_env: str = "FASTWEIGH_API_KEY"
    timeout_seconds: int = 45
    page_size: int = 500
    max_pages: int = 2000
    sync_window_days: int = 1
    warehouse_path: str = "data/ops_intelligence.duckdb"
    output_dir: str = "output"
    shared_drive_path: str | None = None
    entities: dict[str, EntityConfig]
    alerts: AlertThresholdConfig = Field(default_factory=AlertThresholdConfig)
    email: EmailConfig = Field(default_factory=EmailConfig)
    webhook: WebhookConfig = Field(default_factory=WebhookConfig)

    def api_key(self) -> str:
        api_key = os.getenv(self.api_key_env, "")
        if not api_key:
            raise ValueError(
                f"Missing API key in env var '{self.api_key_env}'. Configure it before running sync."
            )
        return api_key


class ConfigError(RuntimeError):
    pass


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file not found: {path}") from exc
    if not isinstance(content, dict):
        raise ConfigError(f"Config file {path} is empty or invalid")
    return content


@lru_cache(maxsize=1)
def load_config(config_path: str | None = None) -> TenantConfig:
    path = Path(config_path or os.getenv("FASTWEIGH_TENANT_CONFIG", "config/tenant.example.yaml"))
    payload = _load_yaml(path)
    try:
        cfg = TenantConfig.model_validate(payload)
    except ValidationError as exc:
        raise ConfigError(f"Invalid tenant configuration in {path}: {exc}") from exc

    Path(cfg.output_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.warehouse_path).parent.mkdir(parents=True, exist_ok=True)
    return cfg


def env_or_empty(name: str) -> str:
    return os.getenv(name, "")
