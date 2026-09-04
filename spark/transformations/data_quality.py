"""
data_quality.py

Reusable data-quality check functions, shared between:
    - the streaming validation step (spark/streaming/transaction_stream.py)
    - a standalone batch DQ report job (this module's `run_report`)
    - unit tests (tests/test_data_quality.py)

Each check returns a DataFrame of *violating* rows tagged with a reason,
so nothing is silently dropped - callers decide whether to quarantine,
alert on, or simply count these rows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_PAYMENT_METHODS = {"card", "digital_wallet", "cash", "bank_transfer"}
VALID_TRANSACTION_TYPES = {"purchase", "refund", "cancellation"}


def null_transaction_ids(df: DataFrame) -> DataFrame:
    return df.filter(F.col("transaction_id").isNull()).withColumn(
        "dq_reason", F.lit("null_transaction_id")
    )


def duplicate_transaction_ids(df: DataFrame) -> DataFrame:
    dupes = (
        df.groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .select("transaction_id")
    )
    return df.join(dupes, on="transaction_id", how="inner").withColumn(
        "dq_reason", F.lit("duplicate_transaction_id")
    )


def invalid_amounts(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("total_amount").isNull() | (F.abs(F.col("total_amount")) < 0.01)
    ).withColumn("dq_reason", F.lit("invalid_transaction_amount"))


def negative_quantities(df: DataFrame) -> DataFrame:
    return df.filter(F.col("quantity") <= 0).withColumn(
        "dq_reason", F.lit("negative_or_zero_quantity")
    )


def invalid_payment_methods(df: DataFrame) -> DataFrame:
    return df.filter(
        ~F.col("payment_method").isin(list(VALID_PAYMENT_METHODS))
    ).withColumn("dq_reason", F.lit("invalid_payment_method"))


def invalid_transaction_types(df: DataFrame) -> DataFrame:
    return df.filter(
        ~F.col("transaction_type").isin(list(VALID_TRANSACTION_TYPES))
    ).withColumn("dq_reason", F.lit("invalid_transaction_type"))


def future_timestamps(df: DataFrame) -> DataFrame:
    return df.filter(F.col("transaction_timestamp") > F.current_timestamp()).withColumn(
        "dq_reason", F.lit("future_timestamp")
    )


def referential_integrity(
    df: DataFrame, customers_df: DataFrame, products_df: DataFrame, stores_df: DataFrame
) -> DataFrame:
    """Rows whose foreign keys don't exist in the current dimension
    snapshots - a real-world symptom of late-arriving dimension data or
    upstream key changes."""
    bad_customer = df.join(
        customers_df.select("customer_id"), "customer_id", "left_anti"
    )
    bad_product = df.join(products_df.select("product_id"), "product_id", "left_anti")
    bad_store = df.join(stores_df.select("store_id"), "store_id", "left_anti")

    return (
        bad_customer.withColumn("dq_reason", F.lit("invalid_customer_id"))
        .unionByName(
            bad_product.withColumn("dq_reason", F.lit("invalid_product_id")),
            allowMissingColumns=True,
        )
        .unionByName(
            bad_store.withColumn("dq_reason", F.lit("invalid_store_id")),
            allowMissingColumns=True,
        )
    )


def run_report(
    df: DataFrame, customers_df: DataFrame, products_df: DataFrame, stores_df: DataFrame
) -> DataFrame:
    """Union of all violations, for a single data-quality report table /
    dashboard page. Each row keeps its original columns plus dq_reason."""
    checks = [
        null_transaction_ids(df),
        duplicate_transaction_ids(df),
        invalid_amounts(df),
        negative_quantities(df),
        invalid_payment_methods(df),
        invalid_transaction_types(df),
        future_timestamps(df),
        referential_integrity(df, customers_df, products_df, stores_df),
    ]
    report = checks[0]
    for c in checks[1:]:
        report = report.unionByName(c, allowMissingColumns=True)
    return report
