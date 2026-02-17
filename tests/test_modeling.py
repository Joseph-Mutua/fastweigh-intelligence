from __future__ import annotations

import json

import duckdb

from ops_intelligence.warehouse.modeling import run_gold_models, run_silver_models


def _seed(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE bronze_events (
            entity TEXT,
            pulled_at TIMESTAMP,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            record_json TEXT,
            record_updated_at TIMESTAMP
        )
        """
    )

    ticket_payload = {
        "id": "t1",
        "orderId": "o1",
        "customerId": "c1",
        "locationId": "yard-1",
        "laneId": "lane-a",
        "targetWeight": 100,
        "netWeight": 104,
        "checkInTimestamp": "2026-01-15T10:00:00Z",
        "loadedTimestamp": "2026-01-15T10:20:00Z",
        "ticketTimestamp": "2026-01-15T10:28:00Z",
        "dispatchAssignedTimestamp": "2026-01-15T10:40:00Z",
        "podTimestamp": "2026-01-15T11:20:00Z",
        "truckId": "truck-1",
        "haulerId": "h1",
        "status": "CLOSED",
        "lastUpdatedAt": "2026-01-15T12:00:00Z",
    }
    invoice_payload = {
        "id": "i1",
        "customerId": "c1",
        "invoiceDate": "2025-09-01",
        "dueDate": "2025-09-15",
        "amount": 5000,
        "openBalance": 5000,
        "status": "OPEN",
        "lastUpdatedAt": "2026-01-15T12:00:00Z",
    }
    pay_payload = {
        "id": "p1",
        "haulerId": "h1",
        "ticketId": "t1",
        "expectedAmount": 120,
        "paidAmount": 110,
        "payDate": "2026-01-15",
        "lastUpdatedAt": "2026-01-15T12:00:00Z",
    }

    conn.executemany(
        "INSERT INTO bronze_events VALUES (?, NOW(), NOW(), NOW(), ?, NOW())",
        [
            ["tickets", json.dumps(ticket_payload)],
            ["invoices", json.dumps(invoice_payload)],
            ["hauler_pay", json.dumps(pay_payload)],
        ],
    )


def test_modeling_generates_gold_tables() -> None:
    conn = duckdb.connect(":memory:")
    _seed(conn)

    run_silver_models(conn)
    run_gold_models(conn, late_sla_minutes=60)

    plant = conn.execute("SELECT tickets_count, avg_time_in_yard_minutes FROM gold_plant_ops_daily").fetchone()
    dispatch = conn.execute("SELECT deliveries, on_time_delivery_rate FROM gold_dispatch_daily").fetchone()
    billing = conn.execute("SELECT total_open_ar, ar_90_plus FROM gold_billing_ar_daily").fetchone()
    hauler = conn.execute("SELECT loads_completed, pay_variance_pct FROM gold_hauler_productivity_daily").fetchone()

    assert plant is not None
    assert plant[0] == 1
    assert round(plant[1], 1) == 20.0

    assert dispatch is not None
    assert dispatch[0] == 1
    assert round(dispatch[1], 2) == 1.0

    assert billing is not None
    assert billing[0] == 5000
    assert billing[1] == 5000

    assert hauler is not None
    assert hauler[0] == 1
    assert round(hauler[1], 2) == round(abs((110 - 120) / 120) * 100, 2)
