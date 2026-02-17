# Workflow Map

## Cross-module flow

1. Order created and scheduled (`orders`)
2. Dispatch assignment and truck mobilization (`dispatchEvents`, ticket dispatch timestamps)
3. Yard check-in, loading, and ticket issue (`tickets`)
4. Delivery and POD capture (`tickets.podTimestamp`, dispatch milestones)
5. Invoice and AR lifecycle (`invoices`)
6. Hauler pay calculation and settlement (`haulerPay`)

## Entity relationship map

- `tickets.orderId` -> `orders.id`
- `tickets.customerId` -> `customers.id`
- `dispatchEvents.ticketId` -> `tickets.id`
- `invoices.customerId` -> `customers.id`
- `haulerPay.ticketId` -> `tickets.id`
- `haulerPay.haulerId` / `tickets.haulerId` for pay-to-work reconciliation

## KPI dependency matrix

- Plant Ops KPIs: tickets only (plus optional product/customer dims)
- Dispatch KPIs: tickets dispatch/POD fields and dispatch events
- Billing KPIs: invoices (customer dims optional)
- Hauler KPIs: tickets + haulerPay

## Incremental sync strategy

- Primary mode: daily windows (`sync_window_days`)
- Window filter: `updatedAfter` and `updatedBefore`
- Cursor pagination: `first + after + pageInfo`
- State checkpoint: `sync_state.last_synced_at` per entity

## Schema-change resilience

- Keep per-entity root/pageInfo paths in tenant config
- Baseline schema snapshot in `schema/`
- CI gate fails when type/field removals are detected
