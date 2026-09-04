"""
cloudwatch_metrics.py

Thin wrapper around boto3 CloudWatch's put_metric_data, used by the
monitoring Airflow DAG and the Spark batch DQ job to publish operational
metrics:

    - RetailPlatform/Kafka        ConsumerLagRecords
    - RetailPlatform/Pipeline     FailedRecordsCount
    - RetailPlatform/Pipeline     DataFreshnessMinutes
    - RetailPlatform/DataQuality  RowCount, DQFailureCount

Kept deliberately small: this is a demonstration of *how* metrics would
be wired to CloudWatch in the AWS-deployed version of this platform, not
a full observability SDK. Credentials/region are picked up from the
standard AWS environment (never hardcoded).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger("cloudwatch_metrics")

NAMESPACE = "RetailPlatform"


def _get_client():
    import boto3
    return boto3.client("cloudwatch")


def put_metric(metric_name: str, value: float, unit: str = "Count", dimensions: dict | None = None):
    try:
        client = _get_client()
        dims = [{"Name": k, "Value": v} for k, v in (dimensions or {}).items()]
        client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[{
                "MetricName": metric_name,
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.now(timezone.utc),
                "Dimensions": dims,
            }],
        )
        logger.info("Published metric %s=%s (%s)", metric_name, value, unit)
    except Exception as exc:  # noqa: BLE001 - monitoring must never crash the caller
        logger.warning("Failed to publish CloudWatch metric %s: %s", metric_name, exc)


def publish_kafka_lag(lag_records: int, consumer_group: str):
    put_metric("ConsumerLagRecords", lag_records, dimensions={"ConsumerGroup": consumer_group})


def publish_data_freshness(minutes_stale: float, table: str):
    put_metric("DataFreshnessMinutes", minutes_stale, unit="None", dimensions={"Table": table})


def publish_dq_failure_count(count: int, check_name: str):
    put_metric("DQFailureCount", count, dimensions={"Check": check_name})
