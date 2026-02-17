# Insomnia + GraphQL Guide

## Goal

Accelerate tenant-specific query validation in Insomnia before production sync runs.

## Steps

1. Create a new request to `https://graphql.fast-weigh.com/`.
2. Add header `Authorization: Bearer <API_KEY>`.
3. Add header `Content-Type: application/json`.
4. Paste query from `queries/*.graphql`.
5. Set variables:

```json
{
  "first": 100,
  "after": null,
  "updatedAfter": "2026-01-01T00:00:00Z",
  "updatedBefore": "2026-01-02T00:00:00Z"
}
```

6. Verify payload shape for:
- root path (`data.<entity>.nodes`)
- page info (`data.<entity>.pageInfo`)
- timestamp field (`lastUpdatedAt` or equivalent)

7. If schema differs, update:
- `queries/<entity>.graphql`
- `config/tenant.example.yaml` entity mapping

## Minimum validation checklist

- First page returns rows and `pageInfo`
- Pagination returns additional rows when `hasNextPage = true`
- Window filtering reduces rows as expected
- Required KPI fields are present or mapped fallbacks exist
