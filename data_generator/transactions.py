"""
transactions.py

Generates a single realistic retail transaction event, referencing the
dimension data produced by reference_data.py.

The event envelope follows a standard schema (see kafka/schemas/transaction_event.json):

    event_id, event_type, event_timestamp, source, payload

This separation (envelope vs payload) means new event types can be added
to the same topic family without breaking consumers that only care about
the envelope, and mirrors how you'd design this against a Schema Registry
in a real deployment.

A small, deliberate fraction of "dirty" events is injected (missing
fields, negative quantities, bad payment methods, future timestamps) so
that the Spark validation layer (Phase 6) and data-quality checks
(Phase 13) have real failure modes to catch instead of a suspiciously
perfect stream.
"""

from __future__ import annotations

import csv
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "reference"

PAYMENT_METHODS = ["card", "digital_wallet", "cash", "bank_transfer"]
TRANSACTION_TYPES_WEIGHTS = {"purchase": 0.90, "refund": 0.07, "cancellation": 0.03}

# Probability that a generated event is deliberately malformed, to exercise
# the pipeline's error handling instead of assuming a perfect world.
DIRTY_EVENT_RATE = 0.03


def _load_csv(name: str) -> list[dict]:
    path = DATA_DIR / name
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run `python data_generator/reference_data.py` first."
        )
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


class TransactionGenerator:
    """Stateful generator that keeps dimension data loaded in memory so it
    can cheaply emit many events without re-reading CSVs each time."""

    def __init__(self):
        self.customers = _load_csv("customers.csv")
        self.products = _load_csv("products.csv")
        self.stores = _load_csv("stores.csv")

        # A small pool of "hot" customers is used to make burst / geo
        # anomaly patterns statistically plausible during demos.
        self._burst_customers = random.sample(
            self.customers, k=min(15, len(self.customers))
        )

    def _pick_transaction_type(self) -> str:
        types, weights = zip(*TRANSACTION_TYPES_WEIGHTS.items())
        return random.choices(types, weights=weights)[0]

    def _make_payload(self, force_anomaly: bool = False) -> dict:
        customer = random.choice(
            self._burst_customers if random.random() < 0.1 else self.customers
        )
        product = random.choice(self.products)
        store = random.choice(self.stores)

        quantity = random.randint(1, 5)
        unit_price = float(product["price"])
        transaction_type = self._pick_transaction_type()

        total_amount = round(unit_price * quantity, 2)
        if transaction_type in ("refund", "cancellation"):
            total_amount = -total_amount

        # occasionally inject an artificially high-value transaction so the
        # anomaly detector has real signal to catch
        if force_anomaly or random.random() < 0.01:
            total_amount = round(total_amount * random.uniform(15, 40), 2)

        payload = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "store_id": store["store_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "payment_method": random.choice(PAYMENT_METHODS),
            "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
            "transaction_type": transaction_type,
        }
        return payload

    def _dirty(self, payload: dict) -> dict:
        """Randomly corrupt a payload to simulate real-world bad data."""
        fault = random.choice(
            ["missing_field", "negative_qty", "bad_payment", "future_ts", "null_id"]
        )
        if fault == "missing_field":
            payload.pop(random.choice(["store_id", "product_id"]), None)
        elif fault == "negative_qty":
            payload["quantity"] = -abs(payload["quantity"])
        elif fault == "bad_payment":
            payload["payment_method"] = "bitcoin"  # not in allowed set
        elif fault == "future_ts":
            future = datetime.now(timezone.utc) + timedelta(days=5)
            payload["transaction_timestamp"] = future.isoformat()
        elif fault == "null_id":
            payload["customer_id"] = None
        return payload

    def generate_event(self) -> dict:
        payload = self._make_payload()
        if random.random() < DIRTY_EVENT_RATE:
            payload = self._dirty(payload)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "transaction",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pos-simulator",
            "payload": payload,
        }
        return event

    def generate_burst(self, n: int = 5) -> list[dict]:
        """Simulate a rapid burst of transactions from the same customer -
        the pattern the transaction-burst anomaly rule is designed to catch."""
        customer = random.choice(self._burst_customers)
        events = []
        for _ in range(n):
            payload = self._make_payload()
            payload["customer_id"] = customer["customer_id"]
            events.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "transaction",
                    "event_timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "pos-simulator",
                    "payload": payload,
                }
            )
        return events


if __name__ == "__main__":
    gen = TransactionGenerator()
    for _ in range(5):
        print(gen.generate_event())
        time.sleep(0.2)
