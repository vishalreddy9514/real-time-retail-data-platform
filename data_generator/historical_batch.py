"""
historical_batch.py

Generates a large historical transaction dataset (batch, not streamed)
covering the last N days, written directly to partitioned Parquet under
data/historical/ in year/month/day layout - the same partitioning
convention used by the S3 raw zone (see Phase 7 / architecture docs).

This represents the "backfill" side of the platform: the data a company
would already have in its warehouse before real-time capture went live,
and is what Airflow's historical ingestion DAG loads into Snowflake.

Run with --rows to control volume, e.g.:
    python historical_batch.py --days 180 --rows-per-day 20000
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
REFERENCE_DIR = DATA_DIR / "reference"
HISTORICAL_DIR = DATA_DIR / "historical"

PAYMENT_METHODS = ["card", "digital_wallet", "cash", "bank_transfer"]
TRANSACTION_TYPES_WEIGHTS = {"purchase": 0.90, "refund": 0.07, "cancellation": 0.03}


def _load_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(REFERENCE_DIR / name)


def generate_day(
    day: datetime,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
    rows_per_day: int,
) -> pd.DataFrame:
    types, weights = zip(*TRANSACTION_TYPES_WEIGHTS.items())

    cust_sample = customers.sample(rows_per_day, replace=True).reset_index(drop=True)
    prod_sample = products.sample(rows_per_day, replace=True).reset_index(drop=True)
    store_sample = stores.sample(rows_per_day, replace=True).reset_index(drop=True)

    quantity = pd.Series([random.randint(1, 5) for _ in range(rows_per_day)])
    unit_price = prod_sample["price"]
    txn_type = pd.Series(random.choices(types, weights=weights, k=rows_per_day))

    total_amount = (unit_price * quantity).round(2)
    refund_mask = txn_type.isin(["refund", "cancellation"])
    total_amount = total_amount.where(~refund_mask, -total_amount)

    # random second-level timestamps spread across the day, weighted toward
    # lunchtime / early-evening "busy periods" for realism
    hour_weights = [
        1,
        1,
        1,
        1,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        8,
        9,
        7,
        6,
        6,
        7,
        9,
        8,
        6,
        4,
        2,
        1,
    ]
    hours = random.choices(range(24), weights=hour_weights, k=rows_per_day)
    timestamps = [
        day.replace(hour=h, minute=random.randint(0, 59), second=random.randint(0, 59))
        for h in hours
    ]

    df = pd.DataFrame(
        {
            "transaction_id": [
                f"TXN-{uuid.uuid4().hex[:12].upper()}" for _ in range(rows_per_day)
            ],
            "customer_id": cust_sample["customer_id"],
            "product_id": prod_sample["product_id"],
            "store_id": store_sample["store_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "payment_method": random.choices(PAYMENT_METHODS, k=rows_per_day),
            "transaction_timestamp": timestamps,
            "transaction_type": txn_type,
            "year": day.year,
            "month": day.month,
            "day": day.day,
        }
    )
    return df


def main(days: int, rows_per_day: int):
    customers = _load_csv("customers.csv")
    products = _load_csv("products.csv")
    stores = _load_csv("stores.csv")

    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    total_rows = 0

    for offset in range(days):
        day = end - timedelta(days=offset)
        df = generate_day(day, customers, products, stores, rows_per_day)

        out_dir = (
            HISTORICAL_DIR
            / f"year={day.year}"
            / f"month={day.month:02d}"
            / f"day={day.day:02d}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "transactions.parquet"
        df.drop(columns=["year", "month", "day"]).to_parquet(out_path, index=False)

        total_rows += len(df)
        if offset % 30 == 0:
            print(
                f"[{offset}/{days}] {day.date()} -> {len(df)} rows written to {out_path}"
            )

    print(
        f"Done. Generated {total_rows:,} historical rows across {days} days -> {HISTORICAL_DIR}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate historical retail transaction batch data"
    )
    parser.add_argument(
        "--days", type=int, default=180, help="Number of historical days to generate"
    )
    parser.add_argument("--rows-per-day", type=int, default=20000, help="Rows per day")
    args = parser.parse_args()
    main(args.days, args.rows_per_day)
