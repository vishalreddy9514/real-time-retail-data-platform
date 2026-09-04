#!/usr/bin/env bash
# create_topics.sh
#
# Creates the Kafka topics used by the platform, with partitioning and
# replication chosen deliberately rather than left at defaults.
#
# Topic design summary
# ---------------------------------------------------------------------------
# transactions   : purchases/refunds/cancellations from POS/e-commerce.
#                  6 partitions, keyed by customer_id, so all events for one
#                  customer stay ordered on the same partition (required for
#                  burst/geo anomaly detection). Replication factor 3 in
#                  production (1 for local single-broker dev).
# payments       : payment-gateway confirmation events, decoupled from the
#                  transaction record itself (payment can fail/retry
#                  independently of the order). 3 partitions, keyed by
#                  transaction_id.
# refunds        : refund lifecycle events (requested/approved/completed).
#                  3 partitions, keyed by transaction_id.
# customers      : CDC-style change events for the customer dimension.
#                  3 partitions, keyed by customer_id, compacted (latest
#                  state per key is enough - full history isn't needed for
#                  dimension joins).
# products       : CDC-style change events for the product dimension.
#                  3 partitions, keyed by product_id, compacted.
#
# Partition counts are sized for local demo throughput, not petabyte scale -
# the point is to show partitioning-by-key is deliberate, not that the
# exact number is load-tested.
# ---------------------------------------------------------------------------

set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"   # set to 3 in a real multi-broker cluster

create_topic () {
  local name="$1"
  local partitions="$2"
  local extra_config="${3:-}"

  echo "Creating topic: ${name} (partitions=${partitions}, replication=${REPLICATION})"
  kafka-topics.sh --bootstrap-server "${BROKER}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION}" \
    ${extra_config}
}

create_topic "transactions" 6
create_topic "payments" 3
create_topic "refunds" 3
create_topic "customers" 3 "--config cleanup.policy=compact"
create_topic "products" 3 "--config cleanup.policy=compact"

echo "Done. Current topics:"
kafka-topics.sh --bootstrap-server "${BROKER}" --list
