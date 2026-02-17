from __future__ import annotations

from datetime import date, datetime
from typing import Any


def deep_get(payload: dict[str, Any], dotted_path: str) -> Any:
    value: Any = payload
    for key in dotted_path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
        if value is None:
            return None
    return value


def iso_day(day: date) -> str:
    return f"{day.isoformat()}T00:00:00Z"


def to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)
