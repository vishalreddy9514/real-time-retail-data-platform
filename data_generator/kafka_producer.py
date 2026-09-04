"""
kafka_producer.py

Continuously produces synthetic transaction events onto the `transactions`
Kafka topic.

Design decisions:
    - Partition key = customer_id. This guarantees all events for a given
      customer land on the same partition, preserving per-customer event
      ordering, which the burst / geo-anomaly detectors depend on.
    - `acks=all` + `retries` gives at-least-once delivery from the
      producer side; downstream de-duplication (Phase 6, on event_id)
      handles the resulting duplicates rather than pretending
      exactly-once is free.
    - Configuration is environment-driven (12-factor style) so the same
      code runs unchanged against local Docker Kafka or a hosted
      cluster (e.g. Confluent Cloud / MSK) in "AWS production" mode.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from transactions import TransactionGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))
BURST_PROBABILITY = float(os.getenv("BURST_PROBABILITY", "0.02"))

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Shutdown signal received (%s). Draining producer...", signum)
    _shutdown = True


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=20,
        compression_type="gzip",
    )


def send_event(producer: KafkaProducer, event: dict) -> None:
    key = (event.get("payload") or {}).get("customer_id") or "unknown"
    try:
        future = producer.send(TOPIC_TRANSACTIONS, key=key, value=event)
        future.add_errback(
            lambda exc: logger.error("Delivery failed for %s: %s", event["event_id"], exc)
        )
    except KafkaError as exc:
        logger.error("Producer error: %s", exc)


def run():
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
    producer = build_producer()
    generator = TransactionGenerator()

    sleep_interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    start = time.time()

    logger.info(
        "Starting event generation: ~%.2f events/sec -> topic '%s'",
        EVENTS_PER_SECOND, TOPIC_TRANSACTIONS,
    )

    while not _shutdown:
        if random.random() < BURST_PROBABILITY:
            for evt in generator.generate_burst(n=random.randint(4, 10)):
                send_event(producer, evt)
                sent += 1
        else:
            send_event(producer, generator.generate_event())
            sent += 1

        if sent % 100 == 0:
            elapsed = time.time() - start
            logger.info("Sent %d events (%.2f events/sec actual)", sent, sent / elapsed)

        time.sleep(sleep_interval)

    logger.info("Flushing producer and shutting down. Total events sent: %d", sent)
    producer.flush(timeout=10)
    producer.close()


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(__file__))
    run()
