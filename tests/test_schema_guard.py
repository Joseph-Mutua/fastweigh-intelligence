from __future__ import annotations

from ops_intelligence.graphql.schema_guard import detect_breaking_changes


def test_detect_breaking_changes() -> None:
    baseline = {
        "types": [
            {"name": "Ticket", "fields": [{"name": "id"}, {"name": "status"}]},
            {"name": "Order", "fields": [{"name": "id"}]},
        ]
    }
    candidate = {
        "types": [
            {"name": "Ticket", "fields": [{"name": "id"}]},
        ]
    }

    issues = detect_breaking_changes(baseline, candidate)

    assert "Type removed: Order" in issues
    assert "Field removed: Ticket.status" in issues
