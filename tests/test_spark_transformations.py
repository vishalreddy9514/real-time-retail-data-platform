"""
PySpark tests for the streaming validation logic and data-quality checks.

Uses a local SparkSession (no cluster / Kafka required) so these run
quickly in CI. Marked with the `spark` pytest marker so they can be
selected/skipped independently of the lighter unit tests if PySpark
isn't installed in a given CI runner.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark" / "transformations"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark" / "streaming"))

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-retail-platform")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_transactions(spark):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    rows = [
        # valid purchase
        ("TXN-1", "CUST-1", "PROD-1", "STORE-1", 2, 10.0, 20.0, "card", now, "purchase"),
        # negative quantity -> invalid
        ("TXN-2", "CUST-2", "PROD-1", "STORE-1", -1, 10.0, -10.0, "card", now, "purchase"),
        # invalid payment method
        ("TXN-3", "CUST-3", "PROD-1", "STORE-1", 1, 10.0, 10.0, "bitcoin", now, "purchase"),
        # future timestamp
        ("TXN-4", "CUST-4", "PROD-1", "STORE-1", 1, 10.0, 10.0, "card", now + timedelta(days=2), "purchase"),
        # duplicate of TXN-1
        ("TXN-1", "CUST-1", "PROD-1", "STORE-1", 2, 10.0, 20.0, "card", now, "purchase"),
    ]
    columns = ["transaction_id", "customer_id", "product_id", "store_id", "quantity",
               "unit_price", "total_amount", "payment_method", "transaction_timestamp",
               "transaction_type"]
    return spark.createDataFrame(rows, columns)


def test_negative_quantities_are_flagged(sample_transactions):
    from data_quality import negative_quantities

    flagged = negative_quantities(sample_transactions)
    ids = {r.transaction_id for r in flagged.collect()}
    assert "TXN-2" in ids


def test_invalid_payment_methods_are_flagged(sample_transactions):
    from data_quality import invalid_payment_methods

    flagged = invalid_payment_methods(sample_transactions)
    ids = {r.transaction_id for r in flagged.collect()}
    assert "TXN-3" in ids


def test_future_timestamps_are_flagged(sample_transactions):
    from data_quality import future_timestamps

    flagged = future_timestamps(sample_transactions)
    ids = {r.transaction_id for r in flagged.collect()}
    assert "TXN-4" in ids


def test_duplicate_transaction_ids_are_flagged(sample_transactions):
    from data_quality import duplicate_transaction_ids

    flagged = duplicate_transaction_ids(sample_transactions)
    ids = [r.transaction_id for r in flagged.collect()]
    assert ids.count("TXN-1") == 2  # both copies of the duplicate are returned
