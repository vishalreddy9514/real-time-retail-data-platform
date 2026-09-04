"""
transaction_stream.py

Core Spark Structured Streaming job for the platform.

Pipeline stages
---------------
1. Read raw JSON events from the `transactions` Kafka topic.
2. Parse against the known schema; anything that fails to parse or fails
   business validation is routed to a `bad_records` sink instead of being
   silently dropped (Phase 13 requirement: bad records must be visible).
3. De-duplicate on event_id within the watermark window (at-least-once
   producer delivery -> exactly-once-ish processing here).
4. Write validated, flattened records to the S3 (or local) raw/processed
   zone, partitioned by event date - this is the batch-queryable source
   of truth that Snowflake loads from.
5. Compute three windowed streaming aggregations (sales-by-minute,
   sales-by-city, product performance) using watermarking so late data is
   handled deterministically instead of accumulating state forever.
6. Apply rule-based anomaly detection (see spark/streaming/anomaly_detection.py)
   on the same validated stream.

Why watermarking + windowing here (and not just in batch)
-----------------------------------------------------------
Retail POS/e-commerce events can arrive slightly out of order (mobile
network retries, multi-region ingestion). A watermark of 2 minutes means
Spark will keep aggregation state open long enough to accept reasonably
late events, then finalize and evict state for windows older than that -
without watermarking, streaming state would grow unbounded and eventually
exhaust executor memory.

Why a single watermark, defined once, on transaction_timestamp
-----------------------------------------------------------------
Spark disallows redefining a watermark on a different column later in
the same query's lineage ("Redefining watermark is disallowed"). All of
this job's downstream windowed aggregations window on
transaction_timestamp, so the watermark is defined exactly once, here,
in deduplicate() - every downstream stage (sales_by_minute,
sales_by_city, product_performance) reuses that same watermark rather
than calling withWatermark() again.

Why checkpointing
-------------------
Checkpointing (to a durable location, e.g. S3) stores the stream's
progress (Kafka offsets + aggregation state) so that if the Spark
application restarts (deploy, node failure, etc.) it resumes exactly
where it left off rather than reprocessing or dropping data - this is
what makes the pipeline fault-tolerant end-to-end.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, DoubleType, IntegerType,
)

import anomaly_detection

# ---------------------------------------------------------------------------
# Configuration (environment-driven so local Docker and AWS EMR/Glue share code)
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")

RAW_LAKE_PATH = os.getenv("RAW_LAKE_PATH", "s3a://retail-data-platform/raw/transactions")
BAD_RECORDS_PATH = os.getenv("BAD_RECORDS_PATH", "s3a://retail-data-platform/raw/bad_records")
AGG_MINUTE_PATH = os.getenv("AGG_MINUTE_PATH", "s3a://retail-data-platform/curated/sales_by_minute")
AGG_CITY_PATH = os.getenv("AGG_CITY_PATH", "s3a://retail-data-platform/curated/sales_by_city")
AGG_PRODUCT_PATH = os.getenv("AGG_PRODUCT_PATH", "s3a://retail-data-platform/curated/product_performance")
ANOMALIES_PATH = os.getenv("ANOMALIES_PATH", "s3a://retail-data-platform/curated/anomalies")

CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "s3a://retail-data-platform/checkpoints")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "2 minutes")
HIGH_VALUE_THRESHOLD = float(os.getenv("HIGH_VALUE_THRESHOLD", "500.0"))

VALID_PAYMENT_METHODS = {"card", "digital_wallet", "cash", "bank_transfer"}
VALID_TRANSACTION_TYPES = {"purchase", "refund", "cancellation"}

# ---------------------------------------------------------------------------
# Schema (mirrors kafka/schemas/transaction_event.json)
# ---------------------------------------------------------------------------

PAYLOAD_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("transaction_type", StringType(), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("payload", PAYLOAD_SCHEMA, True),
])


def build_spark_session(app_name: str = "retail-transaction-stream") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
        .config("spark.sql.streaming.statefulOperator.allowMultiple", "false")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df: DataFrame) -> DataFrame:
    """Parse the Kafka value column as JSON against EVENT_SCHEMA.

    Records that fail to parse produce nulls in `payload`; those rows are
    separated out downstream rather than crashing the query.
    """
    return (
        raw_df.selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value",
                           "timestamp as kafka_timestamp")
        .withColumn("parsed", F.from_json(F.col("json_value"), EVENT_SCHEMA))
        .select("kafka_key", "kafka_timestamp", "json_value", "parsed.*")
    )


def split_valid_and_bad(parsed_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Apply business validation rules and split the stream into a
    validated set and a bad-records set, each carrying the reason for
    rejection so the data-quality report (Phase 13) is actionable."""

    flat = parsed_df.select(
        "event_id", "event_type", "event_timestamp", "source", "kafka_timestamp", "json_value",
        F.col("payload.transaction_id").alias("transaction_id"),
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.store_id").alias("store_id"),
        F.col("payload.quantity").alias("quantity"),
        F.col("payload.unit_price").alias("unit_price"),
        F.col("payload.total_amount").alias("total_amount"),
        F.col("payload.payment_method").alias("payment_method"),
        F.to_timestamp("payload.transaction_timestamp").alias("transaction_timestamp"),
        F.col("payload.transaction_type").alias("transaction_type"),
    )

    validation_reason = (
        F.when(F.col("event_id").isNull(), "unparseable_event")
        .when(F.col("transaction_id").isNull(), "missing_transaction_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("store_id").isNull(), "missing_store_id")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(~F.col("payment_method").isin(list(VALID_PAYMENT_METHODS)), "invalid_payment_method")
        .when(~F.col("transaction_type").isin(list(VALID_TRANSACTION_TYPES)), "invalid_transaction_type")
        .when(F.col("transaction_timestamp") > F.current_timestamp(), "future_timestamp")
        .otherwise(None)
    )

    validated_all = flat.withColumn("rejection_reason", validation_reason)

    good_df = validated_all.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "json_value")
    bad_df = validated_all.filter(F.col("rejection_reason").isNotNull())

    return good_df, bad_df


def deduplicate(df: DataFrame) -> DataFrame:
    """Drop duplicate event_ids within the watermark window - handles
    at-least-once redelivery from the producer/broker without requiring
    an external dedup store.

    Watermark is defined once here on transaction_timestamp (not
    kafka_timestamp) because that's the column all downstream windowed
    aggregations actually window on - Spark disallows redefining a
    watermark on a different column later in the same query lineage, so
    every downstream stage reuses this single watermark rather than
    calling withWatermark() again."""
    return (
        df.withWatermark("transaction_timestamp", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )


def sales_by_minute(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("transaction_type") == "purchase")
        .groupBy(F.window("transaction_timestamp", "1 minute"))
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("total_amount").alias("total_sales"),
            F.avg("total_amount").alias("average_transaction_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_transactions", "total_sales", "average_transaction_value",
        )
    )


def sales_by_city(df: DataFrame, stores_df: DataFrame) -> DataFrame:
    joined = df.filter(F.col("transaction_type") == "purchase").join(
        F.broadcast(stores_df), on="store_id", how="left"
    )
    return (
        joined.groupBy(F.window("transaction_timestamp", "5 minutes"), "city")
        .agg(
            F.count("transaction_id").alias("transactions"),
            F.sum("total_amount").alias("revenue"),
        )
        .select(F.col("window.start").alias("window_start"), "city", "transactions", "revenue")
    )


def product_performance(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("transaction_type") == "purchase")
        .groupBy(F.window("transaction_timestamp", "5 minutes"), "product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
        )
        .select(F.col("window.start").alias("window_start"), "product_id", "units_sold", "revenue")
    )


def write_stream(df: DataFrame, path: str, checkpoint_subdir: str, output_mode: str = "append",
                  trigger_seconds: int = 30):
    return (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{checkpoint_subdir}")
        .outputMode(output_mode)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka_stream(spark)
    parsed = parse_events(raw)
    good_df, bad_df = split_valid_and_bad(parsed)
    deduped = deduplicate(good_df)

    # Small dimension broadcast for city enrichment. In production this
    # would be refreshed periodically from Snowflake/S3 rather than a
    # static local CSV.
    stores_path = os.getenv("STORES_REFERENCE_PATH", "data/reference/stores.csv")
    stores_df = spark.read.option("header", True).csv(stores_path).select("store_id", "city")

    anomaly_query = (
        deduped.writeStream
        .foreachBatch(
            lambda micro_batch_df, batch_id: anomaly_detection.run_in_foreach_batch(
                micro_batch_df, batch_id, stores_df, ANOMALIES_PATH
            )
        )
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/anomalies")
        .trigger(processingTime="30 seconds")
        .start()
    )

    queries = [
        write_stream(deduped, RAW_LAKE_PATH, "raw_transactions"),
        write_stream(bad_df, BAD_RECORDS_PATH, "bad_records"),
        write_stream(sales_by_minute(deduped), AGG_MINUTE_PATH, "sales_by_minute", output_mode="append"),
        write_stream(sales_by_city(deduped, stores_df), AGG_CITY_PATH, "sales_by_city", output_mode="append"),
        write_stream(product_performance(deduped), AGG_PRODUCT_PATH, "product_performance", output_mode="append"),
        anomaly_query,
    ]

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()