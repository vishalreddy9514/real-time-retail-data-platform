"""
historical_ingestion_dag.py

Batch-loads the historical Parquet dataset (data/historical/, or its S3
equivalent) into Snowflake RAW staging tables. Runs once for backfill / on
demand - not on the streaming hot path.

This DAG is deliberately separate from the streaming pipeline: Airflow
orchestrates scheduled, bounded batch work (backfills, warehouse loads,
dbt runs); Kafka + Spark Structured Streaming handle unbounded,
low-latency event processing. Trying to make Airflow "drive" the stream
would reintroduce polling latency and lose Spark's checkpointed
exactly-once semantics - see README "Why streaming and orchestration are
separated" for the full rationale.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _validate_historical_files(**context):
    """Lightweight pre-flight check: fail fast with a clear message rather
    than letting a missing/empty dataset fail deep inside a Spark job."""
    from pathlib import Path

    historical_dir = Path("/opt/airflow/data/historical")
    if not historical_dir.exists() or not any(historical_dir.rglob("*.parquet")):
        raise FileNotFoundError(
            "No historical parquet files found under data/historical/. "
            "Run `python data_generator/historical_batch.py` first."
        )
    file_count = len(list(historical_dir.rglob("*.parquet")))
    print(f"Found {file_count} historical parquet files ready to load.")


with DAG(
    dag_id="historical_ingestion",
    description="Backfill historical transaction data into Snowflake RAW",
    default_args=default_args,
    schedule_interval=None,  # triggered manually / on demand, not continuously
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "batch", "snowflake"],
) as dag:

    validate_files = PythonOperator(
        task_id="validate_historical_files",
        python_callable=_validate_historical_files,
    )

    load_reference_data = BashOperator(
        task_id="load_reference_data",
        bash_command=(
            "spark-submit --master local[*] "
            "/opt/airflow/spark/jobs/load_reference_to_snowflake.py"
        ),
    )

    load_historical_transactions = BashOperator(
        task_id="load_historical_transactions",
        bash_command=(
            "spark-submit --master local[*] "
            "/opt/airflow/spark/jobs/load_to_snowflake.py "
            "--source data/historical --table STG_TRANSACTIONS"
        ),
    )

    row_count_check = BashOperator(
        task_id="post_load_row_count_check",
        bash_command=(
            'snowsql -q "SELECT COUNT(*) FROM RETAIL_PLATFORM.RAW.STG_TRANSACTIONS;" '
            "-o output_format=plain"
        ),
    )

    validate_files >> load_reference_data >> load_historical_transactions >> row_count_check
