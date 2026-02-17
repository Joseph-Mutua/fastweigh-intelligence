# KPI Cookbook

## Purpose

Standard KPI definitions mapped to Fast-Weigh workflows across Tickets, Orders, Dispatch, Billing/AR, and Hauler Pay.

## KPI Catalog

### Plant + Scale Operations

1. **Time-in-yard (minutes)**
- Formula: `loaded_ts - check_in_ts`
- Grain: ticket, aggregated daily by location
- Source entities: tickets
- Required fields: `checkInTimestamp`, `loadedTimestamp`, `locationId`

2. **Time-to-ticket (minutes)**
- Formula: `ticket_ts - loaded_ts`
- Grain: ticket, aggregated daily by location
- Source entities: tickets
- Required fields: `loadedTimestamp`, `ticketTimestamp`

3. **Load variance (%)**
- Formula: `abs((net_weight - target_weight) / target_weight) * 100`
- Grain: ticket, aggregated daily by location
- Source entities: tickets
- Required fields: `targetWeight`, `netWeight`

4. **Tickets per lane-hour**
- Formula: `ticket_count / sum(active_lane_hours)`
- Grain: daily by location
- Source entities: tickets
- Required fields: `ticketId`, `laneId`, event timestamps

### Dispatch Delivery Performance

5. **On-time delivery rate**
- Formula: `deliveries meeting SLA / total deliveries`
- SLA reference implementation: 90 minutes from dispatch assignment to POD
- Source entities: tickets + POD timestamps
- Required fields: `dispatchAssignedTimestamp`, `podTimestamp`

6. **Average delivery duration (minutes)**
- Formula: `avg(pod_ts - dispatch_assigned_ts)`
- Grain: daily by location
- Source entities: tickets, dispatch events (optional enrichment)

7. **Truck/hauler utilization proxy**
- Formula: distinct active trucks and haulers per day/location
- Grain: daily by location
- Source entities: tickets, dispatch events

### Billing / AR Visibility

8. **Total open AR**
- Formula: `sum(open_balance)`
- Grain: as-of date
- Source entities: invoices

9. **AR aging buckets**
- Buckets: current, 1-30, 31-60, 61-90, 90+
- Source entities: invoices
- Required fields: `dueDate`, `openBalance`

10. **AR risk flag**
- Formula: `ar_90_plus > threshold`
- Threshold default: 10,000

### Hauler Productivity + Pay Accuracy

11. **Loads completed**
- Formula: count distinct ticket IDs
- Grain: day + hauler

12. **Active delivery minutes**
- Formula: sum of dispatch-to-POD minutes
- Grain: day + hauler

13. **Pay variance (%)**
- Formula: `abs((paid - expected) / expected) * 100`
- Grain: day + hauler
- Source entities: hauler pay

## Mapping to Fast-Weigh module semantics

- Tickets module -> plant throughput + load accuracy
- Orders module -> job/phase context for demand and status views
- Dispatch module -> assignment, GPS/POD delivery KPIs
- Billing/AR module -> open AR and aging views
- Hauler Pay module -> pay accuracy and productivity analytics

## Data quality expectations

- Timestamps must be normalized to UTC in bronze/silver layers.
- Unit consistency (tons/yards) must be standardized in silver models where mixed units exist.
- Missing timestamps should be excluded from duration KPIs and counted in quality monitoring.

## Recommended KPI targets (starter defaults)

- Time-in-yard <= 45 minutes (site specific)
- Load variance <= 3% average
- On-time delivery >= 92%
- AR 90+ <= 10% of total open AR
- Pay variance <= 1.5%
