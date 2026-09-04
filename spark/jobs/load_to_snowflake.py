"""
load_to_snowflake.py

Batch job (invoked by Airflow, not run continuously) that loads validated
Parquet data from the S3 processed/curated zones into Snowflake staging
tables.

Design note - why this uses the Snowflake Python connector (PUT + COPY
INTO ... MATCH_BY_COLUMN_NAME) instead of the Spark-Snowflake connector's
built-in write path:

The Spark-Snowflake connector's write path stages data as CSV internally
and constructs its own COPY INTO command with a fixed positional column
mapping and a fixed TIMESTAMP_FORMAT. In practice this proved fragile
against this table's TIMESTAMP_TZ columns and its DEFAULT-valued
_loaded_at column, silently skipping every staged row rather than
loading them.

Loading via native Parquet + MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
avoids all of that: Parquet preserves column types exactly (no CSV
format-string guessing), and column-name matching means the file's exact
column count/order no longer needs to match the target table - any
column present in both is loaded, any target column absent from the
file (like _loaded_at, which has a DEFAULT) is simply left at its
default. This is also the more common production pattern for loading
Spark output into Snowflake.

Design note - why PURGE = TRUE on the COPY INTO:

Each run's PUT uploads with a fresh, unique-per-run filename into the
table's internal named stage (@%STG_TRANSACTIONS). Without PURGE,
successfully-loaded files are never removed from that stage - so a
subsequent COPY INTO (which loads everything currently sitting in the
stage, not just the current run's files) silently re-loads every
previous run's files too, duplicating historical data on every run.
PURGE = TRUE deletes each file from the stage immediately after it is
successfully loaded, so every run only ever loads its own fresh data.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S3_PROCESSED_TRANSACTIONS = os.getenv(
    "S3_PROCESSED_TRANSACTIONS", "s3a://retail-data-platform/raw/transactions"
)
LOCAL_EXPORT_DIR = os.getenv("LOCAL_EXPORT_DIR", "/tmp/stg_transactions_export")


def _prepare_transactions(df):
    return df.select(
        "event_id",
        "event_type",
        F.to_timestamp("event_timestamp").alias("event_timestamp"),
        "source",
        "transaction_id",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "unit_price",
        "total_amount",
        "payment_method",
        F.col("transaction_timestamp").alias("transaction_timestamp"),
        "transaction_type",
    )


def _load_via_snowflake_connector(local_dir: str, target_table: str) -> None:
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "RETAIL_PLATFORM"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    try:
        cur = conn.cursor()

        # Belt-and-braces: clear out any files left in the stage from
        # earlier runs (e.g. if a prior run failed after PUT but before
        # COPY, or predates this PURGE fix) before loading fresh files.
        cur.execute(f"REMOVE @%{target_table}")

        parquet_glob = os.path.join(local_dir, "*.parquet").replace("\\", "/")
        cur.execute(
            f"PUT file://{parquet_glob} @%{target_table} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
        )
        cur.execute(
            f"COPY INTO {target_table} "
            f"FILE_FORMAT = (TYPE=PARQUET) "
            f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
            f"PURGE = TRUE"
        )
        result = cur.fetchall()
        print(f"COPY INTO result: {result}")
    finally:
        conn.close()


def load_table(spark: SparkSession, s3_path: str, target_table: str) -> int:
    df = spark.read.parquet(s3_path)
    df = _prepare_transactions(df)
    row_count = df.count()

    (
        df.coalesce(8)
        .write.mode("overwrite")
        .parquet(LOCAL_EXPORT_DIR)
    )

    _load_via_snowflake_connector(LOCAL_EXPORT_DIR, target_table)
    return row_count


def main():
    spark = SparkSession.builder.appName("load-to-snowflake").getOrCreate()

    loaded = load_table(spark, S3_PROCESSED_TRANSACTIONS, "STG_TRANSACTIONS")
    print(f"Loaded {loaded:,} rows into RAW.STG_TRANSACTIONS")

    spark.stop()


if __name__ == "__main__":
    main()