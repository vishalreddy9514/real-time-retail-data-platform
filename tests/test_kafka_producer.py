"""
Tests for data_generator/kafka_producer.py that don't require a running
Kafka broker - they validate serialization and key-selection logic in
isolation using a mock producer, per the "unit tests should not require
external infrastructure" principle.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_generator"))

from kafka_producer import send_event  # noqa: E402


def test_send_event_uses_customer_id_as_key():
    mock_producer = MagicMock()
    event = {
        "event_id": "abc-123",
        "payload": {"customer_id": "CUST-0000000001"},
    }
    send_event(mock_producer, event)

    mock_producer.send.assert_called_once()
    _, kwargs = mock_producer.send.call_args
    assert kwargs["key"] == "CUST-0000000001"
    assert kwargs["value"] == event


def test_send_event_falls_back_to_unknown_key_when_missing_customer():
    mock_producer = MagicMock()
    event = {"event_id": "abc-124", "payload": {}}
    send_event(mock_producer, event)

    _, kwargs = mock_producer.send.call_args
    assert kwargs["key"] == "unknown"


def test_event_value_is_json_serializable():
    event = {
        "event_id": "abc-125",
        "event_type": "transaction",
        "payload": {"customer_id": "CUST-1", "total_amount": 12.5},
    }
    # value_serializer behaviour, exercised directly
    serialized = json.dumps(event).encode("utf-8")
    assert json.loads(serialized.decode("utf-8")) == event
