# Runbook

## Daily operations

1. `ops-intel pipeline`
2. Confirm sync manifest in `output/manifests/`
3. Confirm CSV reports under `output/reports/<timestamp>/`
4. Review alerts in `alert_events` table

## Troubleshooting

### GraphQL auth failures

- Verify `FASTWEIGH_API_KEY`
- Confirm key has GraphQL V2 add-on access in tenant subscription

### Query path errors

- Error indicates invalid `root_path` or `page_info_path`
- Validate response structure in Insomnia and update tenant config

### Empty KPI tables

- Check `bronze_events` row counts by entity
- Confirm date windows include expected transaction dates
- Verify required timestamp/weight fields are returned by query

### Schema check fails

- Run `ops-intel schema-snapshot` for current schema
- Review removed types/fields and patch query/model mappings before deploy

## Recovery

- To rebuild from a specific period:

```powershell
ops-intel sync --start 2026-01-01T00:00:00Z --end 2026-02-01T00:00:00Z
ops-intel model
ops-intel report
ops-intel alerts
```

## SLA ownership recommendations

- Plant Ops manager: yard and load variance alerts
- Dispatch manager: late delivery and utilization alerts
- Finance manager: AR aging alerts
- Integration owner: schema guard and extraction failures
