"""
data_quality_report.py

Batch job that reads the outputs already produced by the streaming
pipeline (raw_transactions, bad_records, anomalies) and computes a
data quality / pipeline health report:

    - records processed (valid, deduplicated transactions)
    - records rejected (failed validation, routed to bad_records)
    - rejection rate
    - anomaly counts by type
    - processing time for this report run
    - last successful run timestamp

The report is written both to a local JSON file (for quick local
inspection / CI artifacts) and to a Snowflake table
(MONITORING.PIPELINE_HEALTH) so it can be queried and visualized in
Power BI alongside the rest of the warehouse.

Why this exists as a separate batch job rather than inline in the
streaming job: computing rejection rate and anomaly breakdowns needs
a full read over the lake's current state, which is a different
access pattern (batch aggregation) than the streaming job's per-
micro-batch processing - keeping them separate means the streaming
job's own uptime doesn't depend on this reporting step, and this can
be safely run on a schedule (e.g. via the existing Airflow
monitoring_dag.py) independent of the stream.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_LAKE_PATH = os.getenv("RAW_LAKE_PATH", "data/lake/raw/transactions")
BAD_RECORDS_PATH = os.getenv("BAD_RECORDS_PATH", "data/lake/raw/bad_records")
ANOMALIES_PATH = os.getenv("ANOMALIES_PATH", "data/lake/curated/anomalies")
REPORT_OUTPUT_DIR = os.getenv("REPORT_OUTPUT_DIR", "data/reports")


def _safe_read_parquet(spark: SparkSession, path: str):
    """Read a Parquet path, returning None if it doesn't exist yet
    (e.g. bad_records may be empty/absent if nothing has ever failed
    validation - that's a good sign, not an error)."""
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def build_report(spark: SparkSession) -> dict:
    start_time = time.time()

    raw_df = _safe_read_parquet(spark, RAW_LAKE_PATH)
    bad_df = _safe_read_parquet(spark, BAD_RECORDS_PATH)
    anomalies_df = _safe_read_parquet(spark, ANOMALIES_PATH)

    records_processed = raw_df.count() if raw_df is not None else 0
    records_rejected = bad_df.count() if bad_df is not None else 0
    total_seen = records_processed + records_rejected
    rejection_rate = (records_rejected / total_seen) if total_seen > 0 else 0.0

    anomaly_breakdown = {}
    total_anomalies = 0
    if anomalies_df is not None and "anomaly_type" in anomalies_df.columns:
        rows = (
            anomalies_df.groupBy("anomaly_type")
            .agg(F.count("*").alias("count"))
            .collect()
        )
        anomaly_breakdown = {r["anomaly_type"]: r["count"] for r in rows}
        total_anomalies = sum(anomaly_breakdown.values())

    processing_seconds = round(time.time() - start_time, 2)

    report = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "records_processed": records_processed,
        "records_rejected": records_rejected,
        "rejection_rate_pct": round(rejection_rate * 100, 3),
        "total_anomalies": total_anomalies,
        "anomaly_breakdown": anomaly_breakdown,
        "report_processing_seconds": processing_seconds,
        "status": "SUCCESS",
    }
    return report


def write_local_report(report: dict) -> str:
    os.makedirs(REPORT_OUTPUT_DIR, exist_ok=True)
    filename = f"data_quality_report_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    path = os.path.join(REPORT_OUTPUT_DIR, filename)
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    return path


def write_to_snowflake(report: dict) -> None:
    """Insert this run's report as one row into MONITORING.PIPELINE_HEALTH.
    Uses a plain INSERT (not PUT/COPY INTO) since this is a single small
    row per run, not a bulk file load."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "RETAIL_PLATFORM"),
        schema="MONITORING",
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MONITORING.PIPELINE_HEALTH
                (run_timestamp, records_processed, records_rejected,
                 rejection_rate_pct, total_anomalies, anomaly_breakdown,
                 report_processing_seconds, status)
            SELECT
                %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s
            """,
            (
                report["run_timestamp"],
                report["records_processed"],
                report["records_rejected"],
                report["rejection_rate_pct"],
                report["total_anomalies"],
                json.dumps(report["anomaly_breakdown"]),
                report["report_processing_seconds"],
                report["status"],
            ),
        )
        print("Inserted pipeline health row into MONITORING.PIPELINE_HEALTH")
    finally:
        conn.close()


def main():
    spark = SparkSession.builder.appName("data-quality-report").getOrCreate()

    report = build_report(spark)
    print(json.dumps(report, indent=2))

    local_path = write_local_report(report)
    print(f"Wrote local report to {local_path}")

    if os.getenv("SNOWFLAKE_ACCOUNT"):
        write_to_snowflake(report)
    else:
        print("SNOWFLAKE_ACCOUNT not set - skipping Snowflake insert (local-only run)")

    spark.stop()


if __name__ == "__main__":
    main()