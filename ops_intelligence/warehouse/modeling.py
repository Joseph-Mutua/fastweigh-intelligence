from __future__ import annotations

import duckdb


def run_silver_models(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE silver_tickets AS
        SELECT
            COALESCE(json_extract_string(record_json, '$.id'), json_extract_string(record_json, '$.ticketId')) AS ticket_id,
            COALESCE(json_extract_string(record_json, '$.orderId'), json_extract_string(record_json, '$.order.id')) AS order_id,
            COALESCE(json_extract_string(record_json, '$.customerId'), json_extract_string(record_json, '$.customer.id')) AS customer_id,
            COALESCE(json_extract_string(record_json, '$.locationId'), json_extract_string(record_json, '$.yardId')) AS location_id,
            COALESCE(json_extract_string(record_json, '$.laneId'), json_extract_string(record_json, '$.scaleLaneId')) AS lane_id,
            COALESCE(json_extract_string(record_json, '$.productId'), json_extract_string(record_json, '$.product.id')) AS product_id,
            COALESCE(json_extract_string(record_json, '$.productName'), json_extract_string(record_json, '$.product.name')) AS product_name,
            COALESCE(json_extract_string(record_json, '$.unitOfMeasure'), json_extract_string(record_json, '$.uom')) AS unit_of_measure,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.targetWeight'), json_extract_string(record_json, '$.targetNetWeight')) AS DOUBLE) AS target_weight,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.netWeight'), json_extract_string(record_json, '$.actualNetWeight')) AS DOUBLE) AS net_weight,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.checkInTimestamp'), json_extract_string(record_json, '$.inYard.checkInAt')) AS TIMESTAMP) AS check_in_ts,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.loadedTimestamp'), json_extract_string(record_json, '$.loadedAt')) AS TIMESTAMP) AS loaded_ts,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.ticketTimestamp'), json_extract_string(record_json, '$.issuedAt')) AS TIMESTAMP) AS ticket_ts,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.dispatchAssignedTimestamp'), json_extract_string(record_json, '$.dispatch.assignedAt')) AS TIMESTAMP) AS dispatch_assigned_ts,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.podTimestamp'), json_extract_string(record_json, '$.proofOfDelivery.deliveredAt')) AS TIMESTAMP) AS pod_ts,
            COALESCE(json_extract_string(record_json, '$.status'), 'UNKNOWN') AS status,
            COALESCE(json_extract_string(record_json, '$.truckId'), json_extract_string(record_json, '$.truck.id')) AS truck_id,
            COALESCE(json_extract_string(record_json, '$.haulerId'), json_extract_string(record_json, '$.hauler.id')) AS hauler_id,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.lastUpdatedAt'), json_extract_string(record_json, '$.updatedAt')) AS TIMESTAMP) AS updated_at
        FROM bronze_events
        WHERE entity = 'tickets'
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE silver_orders AS
        SELECT
            COALESCE(json_extract_string(record_json, '$.id'), json_extract_string(record_json, '$.orderId')) AS order_id,
            COALESCE(json_extract_string(record_json, '$.jobId'), json_extract_string(record_json, '$.job.id')) AS job_id,
            COALESCE(json_extract_string(record_json, '$.phaseId'), json_extract_string(record_json, '$.phase.id')) AS phase_id,
            COALESCE(json_extract_string(record_json, '$.customerId'), json_extract_string(record_json, '$.customer.id')) AS customer_id,
            COALESCE(json_extract_string(record_json, '$.status'), 'UNKNOWN') AS status,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.scheduledDate'), json_extract_string(record_json, '$.dispatchDate')) AS DATE) AS scheduled_date,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.lastUpdatedAt'), json_extract_string(record_json, '$.updatedAt')) AS TIMESTAMP) AS updated_at
        FROM bronze_events
        WHERE entity = 'orders'
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE silver_dispatch_events AS
        SELECT
            COALESCE(json_extract_string(record_json, '$.id'), json_extract_string(record_json, '$.eventId')) AS dispatch_event_id,
            COALESCE(json_extract_string(record_json, '$.ticketId'), json_extract_string(record_json, '$.ticket.id')) AS ticket_id,
            COALESCE(json_extract_string(record_json, '$.truckId'), json_extract_string(record_json, '$.truck.id')) AS truck_id,
            COALESCE(json_extract_string(record_json, '$.haulerId'), json_extract_string(record_json, '$.hauler.id')) AS hauler_id,
            COALESCE(json_extract_string(record_json, '$.eventType'), 'UNKNOWN') AS event_type,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.eventTimestamp'), json_extract_string(record_json, '$.createdAt')) AS TIMESTAMP) AS event_ts,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.latitude'), json_extract_string(record_json, '$.position.latitude')) AS DOUBLE) AS latitude,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.longitude'), json_extract_string(record_json, '$.position.longitude')) AS DOUBLE) AS longitude,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.lastUpdatedAt'), json_extract_string(record_json, '$.updatedAt')) AS TIMESTAMP) AS updated_at
        FROM bronze_events
        WHERE entity = 'dispatch_events'
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE silver_customers AS
        SELECT
            COALESCE(json_extract_string(record_json, '$.id'), json_extract_string(record_json, '$.customerId')) AS customer_id,
            COALESCE(json_extract_string(record_json, '$.name'), json_extract_string(record_json, '$.customerName')) AS customer_name,
            COALESCE(json_extract_string(record_json, '$.segment'), 'Unclassified') AS customer_segment,
            COALESCE(json_extract_string(record_json, '$.region'), 'Unknown') AS region,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.lastUpdatedAt'), json_extract_string(record_json, '$.updatedAt')) AS TIMESTAMP) AS updated_at
        FROM bronze_events
        WHERE entity = 'customers'
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE silver_invoices AS
        SELECT
            COALESCE(json_extract_string(record_json, '$.id'), json_extract_string(record_json, '$.invoiceId')) AS invoice_id,
            COALESCE(json_extract_string(record_json, '$.customerId'), json_extract_string(record_json, '$.customer.id')) AS customer_id,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.invoiceDate'), json_extract_string(record_json, '$.issuedDate')) AS DATE) AS invoice_date,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.dueDate'), json_extract_string(record_json, '$.paymentDueDate')) AS DATE) AS due_date,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.amount'), json_extract_string(record_json, '$.invoiceAmount')) AS DOUBLE) AS invoice_amount,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.openBalance'), json_extract_string(record_json, '$.balanceDue')) AS DOUBLE) AS open_balance,
            COALESCE(json_extract_string(record_json, '$.status'), 'UNKNOWN') AS status,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.lastUpdatedAt'), json_extract_string(record_json, '$.updatedAt')) AS TIMESTAMP) AS updated_at
        FROM bronze_events
        WHERE entity = 'invoices'
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE silver_hauler_pay AS
        SELECT
            COALESCE(json_extract_string(record_json, '$.id'), json_extract_string(record_json, '$.payItemId')) AS pay_item_id,
            COALESCE(json_extract_string(record_json, '$.haulerId'), json_extract_string(record_json, '$.hauler.id')) AS hauler_id,
            COALESCE(json_extract_string(record_json, '$.ticketId'), json_extract_string(record_json, '$.ticket.id')) AS ticket_id,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.expectedAmount'), json_extract_string(record_json, '$.calculatedFreight')) AS DOUBLE) AS expected_amount,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.paidAmount'), json_extract_string(record_json, '$.actualPaidAmount')) AS DOUBLE) AS paid_amount,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.payDate'), json_extract_string(record_json, '$.paidAt')) AS DATE) AS pay_date,
            TRY_CAST(COALESCE(json_extract_string(record_json, '$.lastUpdatedAt'), json_extract_string(record_json, '$.updatedAt')) AS TIMESTAMP) AS updated_at
        FROM bronze_events
        WHERE entity = 'hauler_pay'
        """
    )


def run_gold_models(conn: duckdb.DuckDBPyConnection, late_sla_minutes: int = 90) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE gold_plant_ops_daily AS
        WITH base AS (
            SELECT
                DATE(COALESCE(ticket_ts, loaded_ts, check_in_ts)) AS service_date,
                COALESCE(location_id, 'Unknown') AS location_id,
                lane_id,
                ticket_id,
                check_in_ts,
                loaded_ts,
                ticket_ts,
                target_weight,
                net_weight,
                CASE
                    WHEN check_in_ts IS NOT NULL AND loaded_ts IS NOT NULL
                        THEN DATE_DIFF('minute', check_in_ts, loaded_ts)
                    ELSE NULL
                END AS time_in_yard_minutes,
                CASE
                    WHEN loaded_ts IS NOT NULL AND ticket_ts IS NOT NULL
                        THEN DATE_DIFF('minute', loaded_ts, ticket_ts)
                    ELSE NULL
                END AS time_to_ticket_minutes,
                CASE
                    WHEN target_weight IS NOT NULL AND target_weight <> 0 AND net_weight IS NOT NULL
                        THEN ABS((net_weight - target_weight) / target_weight) * 100
                    ELSE NULL
                END AS load_variance_pct
            FROM silver_tickets
            WHERE COALESCE(ticket_ts, loaded_ts, check_in_ts) IS NOT NULL
        ),
        lane_hours AS (
            SELECT
                service_date,
                location_id,
                lane_id,
                GREATEST(1, DATE_DIFF('hour', MIN(COALESCE(check_in_ts, loaded_ts, ticket_ts)), MAX(COALESCE(ticket_ts, loaded_ts, check_in_ts)))) AS active_hours,
                COUNT(*) AS lane_tickets
            FROM base
            GROUP BY 1,2,3
        )
        SELECT
            b.service_date,
            b.location_id,
            COUNT(DISTINCT b.ticket_id) AS tickets_count,
            AVG(b.time_in_yard_minutes) AS avg_time_in_yard_minutes,
            AVG(b.time_to_ticket_minutes) AS avg_time_to_ticket_minutes,
            AVG(b.load_variance_pct) AS avg_load_variance_pct,
            SUM(CASE WHEN b.load_variance_pct > 5 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS high_variance_rate,
            COUNT(DISTINCT b.lane_id) AS active_lanes,
            SUM(l.active_hours) AS total_lane_hours,
            COUNT(DISTINCT b.ticket_id)::DOUBLE / NULLIF(SUM(l.active_hours), 0) AS tickets_per_lane_hour
        FROM base b
        LEFT JOIN lane_hours l
            ON b.service_date = l.service_date
            AND b.location_id = l.location_id
            AND b.lane_id = l.lane_id
        GROUP BY 1,2
        ORDER BY 1,2
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE gold_dispatch_daily AS
        WITH dispatch_base AS (
            SELECT
                DATE(COALESCE(t.pod_ts, t.dispatch_assigned_ts, t.ticket_ts)) AS service_date,
                COALESCE(t.location_id, 'Unknown') AS location_id,
                t.ticket_id,
                t.truck_id,
                t.hauler_id,
                t.dispatch_assigned_ts,
                t.pod_ts,
                CASE
                    WHEN t.dispatch_assigned_ts IS NOT NULL AND t.pod_ts IS NOT NULL
                        THEN DATE_DIFF('minute', t.dispatch_assigned_ts, t.pod_ts)
                    ELSE NULL
                END AS delivery_minutes,
                CASE
                    WHEN t.dispatch_assigned_ts IS NOT NULL AND t.pod_ts IS NOT NULL
                         AND DATE_DIFF('minute', t.dispatch_assigned_ts, t.pod_ts) <= {late_sla_minutes}
                        THEN 1
                    ELSE 0
                END AS on_time_flag
            FROM silver_tickets t
            WHERE COALESCE(t.pod_ts, t.dispatch_assigned_ts, t.ticket_ts) IS NOT NULL
        )
        SELECT
            service_date,
            location_id,
            COUNT(DISTINCT ticket_id) AS deliveries,
            AVG(delivery_minutes) AS avg_delivery_minutes,
            SUM(on_time_flag)::DOUBLE / NULLIF(COUNT(*), 0) AS on_time_delivery_rate,
            COUNT(DISTINCT truck_id) AS active_trucks,
            COUNT(DISTINCT hauler_id) AS active_haulers
        FROM dispatch_base
        GROUP BY 1,2
        ORDER BY 1,2
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE gold_billing_ar_daily AS
        WITH aged AS (
            SELECT
                CURRENT_DATE AS as_of_date,
                customer_id,
                open_balance,
                CASE
                    WHEN due_date IS NULL THEN 'unknown'
                    WHEN due_date >= CURRENT_DATE THEN 'current'
                    WHEN due_date < CURRENT_DATE AND due_date >= CURRENT_DATE - INTERVAL 30 DAY THEN '1_30'
                    WHEN due_date < CURRENT_DATE - INTERVAL 30 DAY AND due_date >= CURRENT_DATE - INTERVAL 60 DAY THEN '31_60'
                    WHEN due_date < CURRENT_DATE - INTERVAL 60 DAY AND due_date >= CURRENT_DATE - INTERVAL 90 DAY THEN '61_90'
                    ELSE '90_plus'
                END AS aging_bucket
            FROM silver_invoices
            WHERE COALESCE(open_balance, 0) > 0
        )
        SELECT
            as_of_date,
            SUM(open_balance) AS total_open_ar,
            SUM(CASE WHEN aging_bucket = 'current' THEN open_balance ELSE 0 END) AS ar_current,
            SUM(CASE WHEN aging_bucket = '1_30' THEN open_balance ELSE 0 END) AS ar_1_30,
            SUM(CASE WHEN aging_bucket = '31_60' THEN open_balance ELSE 0 END) AS ar_31_60,
            SUM(CASE WHEN aging_bucket = '61_90' THEN open_balance ELSE 0 END) AS ar_61_90,
            SUM(CASE WHEN aging_bucket = '90_plus' THEN open_balance ELSE 0 END) AS ar_90_plus,
            COUNT(DISTINCT customer_id) AS customers_with_open_ar
        FROM aged
        GROUP BY 1
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE gold_hauler_productivity_daily AS
        WITH base AS (
            SELECT
                DATE(COALESCE(t.pod_ts, t.ticket_ts, t.loaded_ts)) AS service_date,
                t.hauler_id,
                t.truck_id,
                t.ticket_id,
                t.dispatch_assigned_ts,
                t.pod_ts,
                CASE
                    WHEN t.dispatch_assigned_ts IS NOT NULL AND t.pod_ts IS NOT NULL
                        THEN DATE_DIFF('minute', t.dispatch_assigned_ts, t.pod_ts)
                    ELSE NULL
                END AS active_delivery_minutes
            FROM silver_tickets t
            WHERE COALESCE(t.pod_ts, t.ticket_ts, t.loaded_ts) IS NOT NULL
        ),
        pay AS (
            SELECT
                DATE(pay_date) AS service_date,
                hauler_id,
                SUM(expected_amount) AS expected_pay,
                SUM(paid_amount) AS paid_pay
            FROM silver_hauler_pay
            GROUP BY 1,2
        )
        SELECT
            b.service_date,
            b.hauler_id,
            COUNT(DISTINCT b.ticket_id) AS loads_completed,
            COUNT(DISTINCT b.truck_id) AS trucks_used,
            SUM(b.active_delivery_minutes) AS active_delivery_minutes,
            p.expected_pay,
            p.paid_pay,
            CASE
                WHEN p.expected_pay IS NULL OR p.expected_pay = 0 THEN NULL
                ELSE ABS((COALESCE(p.paid_pay, 0) - p.expected_pay) / p.expected_pay) * 100
            END AS pay_variance_pct
        FROM base b
        LEFT JOIN pay p
            ON b.service_date = p.service_date
            AND b.hauler_id = p.hauler_id
        GROUP BY 1,2,6,7
        ORDER BY 1,2
        """
    )
