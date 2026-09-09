"""Pure-schema validation for TransactionEventIn — no infra needed.

Covers the two field validators: currency upper-casing and the naive-timestamp-means-UTC
coercion added alongside the naive from/to fix on GET /users/{id}/transactions (see
app/schemas/events.py) — a naive timestamp must mean the same instant regardless of the
process's local timezone, not get silently reinterpreted by it.
"""

from datetime import UTC, datetime, timedelta

from app.schemas.events import TransactionEventIn


def _payload(**overrides: object) -> dict:
    fields = {
        "id": "t1",
        "user_id": "u1",
        "amount": "10.00",
        "currency": "eur",
        "timestamp": "2026-01-01T00:00:00",
    }
    fields.update(overrides)
    return fields


def test_currency_is_upper_cased():
    event = TransactionEventIn(**_payload(currency="eur"))

    assert event.currency == "EUR"


def test_naive_timestamp_is_treated_as_utc():
    event = TransactionEventIn(**_payload(timestamp="2026-01-01T13:00:00"))

    assert event.timestamp == datetime(2026, 1, 1, 13, 0, tzinfo=UTC)


def test_offset_timestamp_keeps_its_own_offset():
    # datetime equality is instant-based, so comparing against a UTC datetime here would
    # pass whether or not the validator actually left +02:00 alone (both represent the same
    # instant) — asserting the offset itself is what actually pins "left alone".
    event = TransactionEventIn(**_payload(timestamp="2026-01-01T13:00:00+02:00"))

    assert event.timestamp.utcoffset() == timedelta(hours=2)
    assert event.timestamp == datetime(2026, 1, 1, 11, 0, tzinfo=UTC)
