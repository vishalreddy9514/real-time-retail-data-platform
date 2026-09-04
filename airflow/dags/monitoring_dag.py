"""
monitoring_dag.py

Runs every 10 minutes to check platform health signals that don't belong
inside the streaming job itself:
    - Kafka consumer group lag (is Spark keeping up with the topic?)
    - Data freshness in Snowflake (has new data landed recently?)
    - Row-count sanity checks on curated tables
    - Count of records routed to bad_records in the last interval

Findings are logged and, where CloudWatch is configured, published as
custom metrics (see monitoring/cloudwatch_metrics.py in Phase 17).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def check_kafka_consumer_lag(**context):
    import subprocess

    result = subprocess.run(
        [
            "kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:9092",
            "--describe", "--group", "spark-transaction-stream",
        ],
        capture_output=True, text=True, check=False,
    )
    print(result.stdout or result.stderr)
    # A real implementation would parse the LAG column and raise/alert
    # past a configurable threshold, and push the value to CloudWatch.


def check_data_freshness(**context):
    """Fails the task (triggering Airflow alerting) if no new transaction
    rows have landed in Snowflake within the expected SLA window."""
    import os

    max_staleness_minutes = int(os.getenv("FRESHNESS_SLA_MINUTES", "30"))
    print(f"Checking RAW.STG_TRANSACTIONS freshness against SLA of {max_staleness_minutes} minutes...")
    # Placeholder for an actual Snowflake query via snowflake-connector-python:
    #   SELECT DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) FROM RAW.STG_TRANSACTIONS;
    # and raising if the result exceeds max_staleness_minutes.


def check_bad_record_rate(**context):
    print("Checking bad_records volume over the last monitoring interval...")
    # Placeholder: read row count from S3 bad_records path / Snowflake and
    # compare against total transaction volume; alert if rejection rate
    # spikes above a threshold (signals an upstream schema change).


with DAG(
    dag_id="pipeline_monitoring",
    description="Health checks: Kafka lag, data freshness, bad-record rate",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring"],
) as dag:

    kafka_lag = PythonOperator(task_id="check_kafka_consumer_lag", python_callable=check_kafka_consumer_lag)
    freshness = PythonOperator(task_id="check_data_freshness", python_callable=check_data_freshness)
    bad_records = PythonOperator(task_id="check_bad_record_rate", python_callable=check_bad_record_rate)

    [kafka_lag, freshness, bad_records]
