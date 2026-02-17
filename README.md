# Fast-Weigh Operations Intelligence Pack

Reference implementation for **Project 2: GraphQL V2 -> KPI Warehouse -> Dashboards + Alerts**.

## What this delivers

- GraphQL V2 extraction with bearer auth and cursor pagination
- Incremental sync by date windows + per-entity state tracking
- Schema guard (introspection snapshot + breaking-change detection)
- Warehouse layers:
  - `bronze`: raw event records
  - `silver`: normalized operational entities
  - `gold`: KPI marts for plant, dispatch, AR, and hauler pay
- Streamlit dashboards:
  - Plant Ops
  - Dispatch
  - Billing/AR
  - Hauler Productivity
- Scheduled outputs:
  - CSV exports
  - shared-drive copy
  - optional email/webhook distribution
- Alert engine:
  - yard congestion
  - load variance
  - late deliveries
  - AR aging risk
- Docker packaging + CI + tests

## Architecture

```text
Fast-Weigh GraphQL V2
        |
        v
Extraction (ops_intelligence/extraction/sync.py)
        |
        v
DuckDB bronze_events + sync_state
        |
        v
Silver models (tickets/orders/dispatch/customers/invoices/hauler_pay)
        |
        v
Gold KPI marts
  - gold_plant_ops_daily
  - gold_dispatch_daily
  - gold_billing_ar_daily
  - gold_hauler_productivity_daily
        |
        +--> Dashboards (Streamlit)
        +--> Scheduled CSV/email/webhook reports
        +--> Alerts (threshold rules)
```

## KPI coverage

Detailed formulas and field mapping are in `docs/kpi-cookbook.md`.

Primary KPIs implemented:

- Time-in-yard (check-in -> loaded)
- Time-to-ticket (loaded -> ticket issued)
- Load variance % (target vs actual net)
- Tickets per lane-hour
- Dispatch on-time rate (dispatch assigned -> POD)
- Average delivery minutes
- AR aging buckets + 90+ risk
- Hauler loads, active delivery time, pay variance %

## Project layout

```text
ops_intelligence/
  alerts/
  dashboard/
  extraction/
  graphql/
  reports/
  scheduler/
  warehouse/
  cli.py
  config.py
  pipeline.py
config/
  tenant.example.yaml
queries/
  *.graphql
docs/
  kpi-cookbook.md
  workflow-map.md
  insomnia-guide.md
  deployment.md
  runbook.md
tests/
  test_*.py
```

## Quick start

1. Create venv + install dependencies.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[dev]
```

2. Configure environment.

```powershell
Copy-Item .env.example .env
$env:FASTWEIGH_API_KEY = "<YOUR_FASTWEIGH_API_KEY>"
$env:FASTWEIGH_TENANT_CONFIG = "config/tenant.example.yaml"
```

3. Review and adjust query files in `queries/` to match your GraphQL V2 schema naming.

4. Run full pipeline.

```powershell
ops-intel pipeline
```

5. Launch dashboard.

```powershell
ops-intel dashboard
```

## CLI commands

```powershell
ops-intel sync --entity tickets --entity orders
ops-intel model
ops-intel report
ops-intel alerts
ops-intel pipeline
ops-intel schema-snapshot --output schema/fastweigh_schema_snapshot.json
ops-intel schema-check --baseline schema/fastweigh_schema_snapshot.json
ops-intel schedule --cron "0 6 * * *"
```

## Schema guard in CI

- Store baseline snapshot at `schema/fastweigh_schema_snapshot.json`
- Run `ops-intel schema-check` in CI to detect removed types/fields before production jobs fail

## Notes on GraphQL compatibility

The query templates in `queries/` are intentionally explicit and may require field/argument adjustment per tenant schema shape. The extraction layer is config-driven:

- query file path
- root path (e.g., `data.tickets.nodes`)
- page info path (e.g., `data.tickets.pageInfo`)
- cursor/date variable names

Adjust these in `config/tenant.example.yaml` without changing code.

## Docker

```powershell
docker build -t fastweigh-ops-intel .
docker run --rm -e FASTWEIGH_API_KEY=<KEY> -e FASTWEIGH_TENANT_CONFIG=config/tenant.example.yaml fastweigh-ops-intel ops-intel pipeline
```

## Validation

```powershell
pytest
ruff check .
```

## Operational adoption package

- KPI cookbook for business stakeholders: `docs/kpi-cookbook.md`
- Workflow-to-data mapping: `docs/workflow-map.md`
- Insomnia guidance for API teams: `docs/insomnia-guide.md`
- Deployment + runbook: `docs/deployment.md`, `docs/runbook.md`
