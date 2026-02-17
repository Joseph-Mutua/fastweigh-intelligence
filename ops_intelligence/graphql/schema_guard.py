from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from graphql import get_introspection_query

from ops_intelligence.graphql.client import GraphQLClient


def introspect_schema(client: GraphQLClient) -> dict[str, Any]:
    query = get_introspection_query(descriptions=True)
    payload = client.execute(query, variables={})
    schema = payload.get("data", {}).get("__schema")
    if not isinstance(schema, dict):
        raise RuntimeError("GraphQL introspection failed: missing __schema")
    return schema


def save_schema_snapshot(schema: dict[str, Any], target_path: str) -> None:
    path = Path(target_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema, indent=2, sort_keys=True), encoding="utf-8")


def load_schema_snapshot(path: str) -> dict[str, Any]:
    snapshot_path = Path(path)
    if not snapshot_path.exists():
        raise FileNotFoundError(f"Schema snapshot not found: {path}")
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def _object_fields(schema: dict[str, Any]) -> dict[str, set[str]]:
    fields: dict[str, set[str]] = {}
    for type_obj in schema.get("types", []):
        name = type_obj.get("name")
        type_fields = type_obj.get("fields")
        if not name or not isinstance(type_fields, list):
            continue
        fields[name] = {f.get("name") for f in type_fields if f.get("name")}
    return fields


def detect_breaking_changes(baseline: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    baseline_types = _object_fields(baseline)
    candidate_types = _object_fields(candidate)

    issues: list[str] = []
    missing_types = sorted(set(baseline_types) - set(candidate_types))
    for missing_type in missing_types:
        issues.append(f"Type removed: {missing_type}")

    for type_name, old_fields in baseline_types.items():
        new_fields = candidate_types.get(type_name, set())
        removed_fields = sorted(old_fields - new_fields)
        for field_name in removed_fields:
            issues.append(f"Field removed: {type_name}.{field_name}")

    return issues
