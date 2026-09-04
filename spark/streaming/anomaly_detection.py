"""
anomaly_detection.py

Transparent, rule-based anomaly detection over the validated transaction
stream. Deliberately NOT a black-box ML model - every flag here can be
explained in one sentence, which matters both for interview discussion
and for a real fraud/ops team who need to trust and act on alerts.

Rules implemented
------------------
1. High-value transaction
       total_amount > HIGH_VALUE_THRESHOLD (configurable)
2. Transaction burst
       >= BURST_COUNT_THRESHOLD transactions from the same customer_id
       within BURST_WINDOW (e.g. 5 in 2 minutes)
3. Geographic anomaly
       Same customer transacting from two different cities within a time
       gap too short to be physically plausible (e.g. < 30 minutes apart
       for cities that aren't the same)
4. Refund anomaly
       A customer's refund count in a rolling window exceeds a threshold
       relative to their purchase count (refund rate spike)

Each detector emits a row onto a common `anomalies` output with:
    anomaly_type, customer_id, detected_at, detail, severity

so the curated `anomalies` table can back a single Power BI "Anomaly
Monitoring" page regardless of which rule fired.

An ML-based extension (e.g. isolation forest on transaction embeddings)
is noted as a documented future improvement, not implemented here - see
README "Future Improvements".
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HIGH_VALUE_THRESHOLD = float(os.getenv("HIGH_VALUE_THRESHOLD", "500.0"))
BURST_WINDOW = os.getenv("BURST_WINDOW", "2 minutes")
BURST_COUNT_THRESHOLD = int(os.getenv("BURST_COUNT_THRESHOLD", "5"))
GEO_ANOMALY_MINUTES = int(os.getenv("GEO_ANOMALY_MINUTES", "30"))
REFUND_RATE_THRESHOLD = float(os.getenv("REFUND_RATE_THRESHOLD", "0.5"))


def high_value_anomalies(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("transaction_type") == "purchase") & (F.col("total_amount") > HIGH_VALUE_THRESHOLD))
        .select(
            F.lit("high_value_transaction").alias("anomaly_type"),
            "customer_id",
            F.col("transaction_timestamp").alias("detected_at"),
            F.concat(F.lit("Transaction "), F.col("transaction_id"),
                     F.lit(" amount="), F.col("total_amount").cast("string")).alias("detail"),
            F.lit("high").alias("severity"),
        )
    )


def burst_anomalies(df: DataFrame) -> DataFrame:
    """Uses a windowed aggregation (not an unbounded stateful window) so
    state is bounded by the watermark, consistent with the main stream."""
    return (
        df.withWatermark("transaction_timestamp", BURST_WINDOW)
        .groupBy(F.window("transaction_timestamp", BURST_WINDOW), "customer_id")
        .agg(F.count("transaction_id").alias("txn_count"))
        .filter(F.col("txn_count") >= BURST_COUNT_THRESHOLD)
        .select(
            F.lit("transaction_burst").alias("anomaly_type"),
            "customer_id",
            F.col("window.end").alias("detected_at"),
            F.concat(F.lit(""), F.col("txn_count").cast("string"),
                     F.lit(" transactions in "), F.lit(BURST_WINDOW)).alias("detail"),
            F.lit("medium").alias("severity"),
        )
    )


def geographic_anomalies(df: DataFrame, stores_df) -> DataFrame:
    """Batch/micro-batch style detector: for each customer, compare
    consecutive transactions (by time) and flag when the city changes
    within an implausible time gap. Implemented with a window function
    over customer_id ordered by time - intended to run inside
    foreachBatch on each streaming micro-batch (see run_in_foreach_batch
    below) since lag() isn't directly supported in pure structured
    streaming without foreachBatch.
    """
    enriched = df.join(F.broadcast(stores_df), on="store_id", how="left")
    w = Window.partitionBy("customer_id").orderBy("transaction_timestamp")

    with_lag = (
        enriched
        .withColumn("prev_city", F.lag("city").over(w))
        .withColumn("prev_ts", F.lag("transaction_timestamp").over(w))
        .withColumn(
            "minutes_since_prev",
            (F.col("transaction_timestamp").cast("long") - F.col("prev_ts").cast("long")) / 60.0,
        )
    )

    flagged = with_lag.filter(
        F.col("prev_city").isNotNull()
        & (F.col("city") != F.col("prev_city"))
        & (F.col("minutes_since_prev") < GEO_ANOMALY_MINUTES)
    )

    return flagged.select(
        F.lit("geographic_anomaly").alias("anomaly_type"),
        "customer_id",
        F.col("transaction_timestamp").alias("detected_at"),
        F.concat(
            F.lit("Moved from "), F.col("prev_city"), F.lit(" to "), F.col("city"),
            F.lit(" in "), F.round("minutes_since_prev", 1).cast("string"), F.lit(" min"),
        ).alias("detail"),
        F.lit("high").alias("severity"),
    )


def refund_rate_anomalies(df: DataFrame) -> DataFrame:
    agg = (
        df.withWatermark("transaction_timestamp", "10 minutes")
        .groupBy(F.window("transaction_timestamp", "10 minutes"), "customer_id")
        .agg(
            F.sum(F.when(F.col("transaction_type") == "refund", 1).otherwise(0)).alias("refunds"),
            F.sum(F.when(F.col("transaction_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        )
        .withColumn(
            "refund_rate",
            F.col("refunds") / F.greatest(F.col("purchases") + F.col("refunds"), F.lit(1)),
        )
        .filter((F.col("refunds") >= 2) & (F.col("refund_rate") >= REFUND_RATE_THRESHOLD))
    )
    return agg.select(
        F.lit("refund_anomaly").alias("anomaly_type"),
        "customer_id",
        F.col("window.end").alias("detected_at"),
        F.concat(F.lit("Refund rate "), F.round(F.col("refund_rate") * 100, 1).cast("string"),
                 F.lit("% over window")).alias("detail"),
        F.lit("medium").alias("severity"),
    )


def run_in_foreach_batch(micro_batch_df: DataFrame, batch_id: int, stores_df, sink_path: str):
    """Called from `foreachBatch` in the main streaming query so that the
    non-streaming-native geographic anomaly logic (window lag) can run on
    each finite micro-batch DataFrame, then union with the other rule
    outputs and append to the anomalies table."""
    anomalies = (
        high_value_anomalies(micro_batch_df)
        .unionByName(burst_anomalies(micro_batch_df), allowMissingColumns=True)
        .unionByName(geographic_anomalies(micro_batch_df, stores_df), allowMissingColumns=True)
        .unionByName(refund_rate_anomalies(micro_batch_df), allowMissingColumns=True)
    )
    (
        anomalies.write.mode("append")
        .partitionBy("anomaly_type")
        .parquet(sink_path)
    )
