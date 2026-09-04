"""
Unit tests for data_generator/transactions.py.

Run with: pytest tests/ -v
Requires reference data to exist first:
    python data_generator/reference_data.py
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_generator"))

from transactions import TransactionGenerator  # noqa: E402


@pytest.fixture(scope="module")
def generator():
    return TransactionGenerator()


def test_event_envelope_has_required_fields(generator):
    event = generator.generate_event()
    for field in ("event_id", "event_type", "event_timestamp", "source", "payload"):
        assert field in event


def test_event_type_is_transaction(generator):
    event = generator.generate_event()
    assert event["event_type"] == "transaction"


def test_payload_has_required_keys_when_clean(generator):
    # Generate several events and check at least one "clean" (non-dirty) one
    # contains all expected payload keys.
    found_clean = False
    for _ in range(200):
        event = generator.generate_event()
        payload = event["payload"]
        expected_keys = {
            "transaction_id", "customer_id", "product_id", "store_id",
            "quantity", "unit_price", "total_amount", "payment_method",
            "transaction_timestamp", "transaction_type",
        }
        if expected_keys.issubset(payload.keys()) and all(v is not None for v in payload.values()):
            found_clean = True
            break
    assert found_clean, "Expected at least one fully-populated clean event in 200 samples"


def test_burst_events_share_customer_id(generator):
    events = generator.generate_burst(n=6)
    customer_ids = {e["payload"]["customer_id"] for e in events if e["payload"].get("customer_id")}
    assert len(customer_ids) == 1, "All burst events should share the same customer_id"


def test_transaction_type_is_valid_when_present(generator):
    valid_types = {"purchase", "refund", "cancellation"}
    for _ in range(100):
        event = generator.generate_event()
        txn_type = event["payload"].get("transaction_type")
        if txn_type is not None:
            assert txn_type in valid_types


def test_refund_and_cancellation_amounts_are_negative(generator):
    for _ in range(300):
        event = generator.generate_event()
        payload = event["payload"]
        if payload.get("transaction_type") in ("refund", "cancellation") and payload.get("total_amount") is not None:
            assert payload["total_amount"] <= 0
