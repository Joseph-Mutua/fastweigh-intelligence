from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

import typer

from ops_intelligence.alerts.engine import run_alert_engine
from ops_intelligence.config import load_config
from ops_intelligence.extraction.sync import sync_entities
from ops_intelligence.graphql.client import GraphQLClient
from ops_intelligence.graphql.schema_guard import (
    detect_breaking_changes,
    introspect_schema,
    load_schema_snapshot,
    save_schema_snapshot,
)
from ops_intelligence.pipeline import run_full_pipeline, run_modeling, run_reporting
from ops_intelligence.scheduler.service import start_scheduler
from ops_intelligence.warehouse.db import connect_warehouse

app = typer.Typer(help="Fast-Weigh Operations Intelligence Pack")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@app.command()
def sync(
    entity: list[str] = typer.Option(None, "--entity", help="Entity name; repeat for multiple"),
    start: str | None = typer.Option(None, help="ISO datetime start"),
    end: str | None = typer.Option(None, help="ISO datetime end"),
    config_path: str | None = typer.Option(None, "--config"),
) -> None:
    cfg = load_config(config_path)
    conn = connect_warehouse(cfg.warehouse_path)
    try:
        counts = sync_entities(
            conn=conn,
            cfg=cfg,
            entities=entity or None,
            start_at=_parse_dt(start),
            end_at=_parse_dt(end),
        )
        typer.echo(json.dumps(counts, indent=2, sort_keys=True))
    finally:
        conn.close()


@app.command()
def model(config_path: str | None = typer.Option(None, "--config")) -> None:
    cfg = load_config(config_path)
    conn = connect_warehouse(cfg.warehouse_path)
    try:
        run_modeling(cfg, conn)
        typer.echo("Modeling complete")
    finally:
        conn.close()


@app.command()
def report(config_path: str | None = typer.Option(None, "--config")) -> None:
    cfg = load_config(config_path)
    conn = connect_warehouse(cfg.warehouse_path)
    try:
        payload = run_reporting(cfg, conn)
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
    finally:
        conn.close()


@app.command()
def alerts(config_path: str | None = typer.Option(None, "--config")) -> None:
    cfg = load_config(config_path)
    conn = connect_warehouse(cfg.warehouse_path)
    try:
        findings = run_alert_engine(conn, cfg)
        typer.echo(json.dumps([f.__dict__ for f in findings], indent=2))
    finally:
        conn.close()


@app.command()
def pipeline(
    entity: list[str] = typer.Option(None, "--entity", help="Entity name; repeat for multiple"),
    start: str | None = typer.Option(None, help="ISO datetime start"),
    end: str | None = typer.Option(None, help="ISO datetime end"),
    config_path: str | None = typer.Option(None, "--config"),
) -> None:
    cfg = load_config(config_path)
    conn = connect_warehouse(cfg.warehouse_path)
    try:
        payload = run_full_pipeline(
            cfg=cfg,
            conn=conn,
            entities=entity or None,
            start_at=_parse_dt(start),
            end_at=_parse_dt(end),
        )
        typer.echo(json.dumps(payload, indent=2, default=str))
    finally:
        conn.close()


@app.command("schema-snapshot")
def schema_snapshot(
    output: str = typer.Option("schema/fastweigh_schema_snapshot.json"),
    config_path: str | None = typer.Option(None, "--config"),
) -> None:
    cfg = load_config(config_path)
    with GraphQLClient(cfg.graphql_endpoint, cfg.api_key(), cfg.timeout_seconds) as client:
        schema = introspect_schema(client)
    save_schema_snapshot(schema, output)
    typer.echo(f"Schema snapshot saved: {output}")


@app.command("schema-check")
def schema_check(
    baseline: str = typer.Option("schema/fastweigh_schema_snapshot.json"),
    config_path: str | None = typer.Option(None, "--config"),
) -> None:
    cfg = load_config(config_path)
    old_schema = load_schema_snapshot(baseline)
    with GraphQLClient(cfg.graphql_endpoint, cfg.api_key(), cfg.timeout_seconds) as client:
        new_schema = introspect_schema(client)

    issues = detect_breaking_changes(old_schema, new_schema)
    if issues:
        for issue in issues:
            typer.echo(f"BREAKING: {issue}")
        raise typer.Exit(code=1)
    typer.echo("No breaking schema changes detected")


@app.command()
def dashboard(config_path: str | None = typer.Option(None, "--config")) -> None:
    env = os.environ.copy()
    if config_path:
        env["FASTWEIGH_TENANT_CONFIG"] = config_path
    app_path = Path(__file__).parent / "dashboard" / "app.py"
    subprocess.run(
        [
            "streamlit",
            "run",
            str(app_path),
            "--server.headless",
            "true",
            "--server.port",
            "8501",
        ],
        env=env,
        check=True,
    )


@app.command()
def schedule(
    cron: str = typer.Option("0 6 * * *", help="Cron schedule (server timezone)"),
    config_path: str | None = typer.Option(None, "--config"),
) -> None:
    cfg = load_config(config_path)
    typer.echo(f"Starting scheduler with cron '{cron}' in timezone {cfg.timezone}")
    start_scheduler(cfg, cron)


if __name__ == "__main__":
    app()
