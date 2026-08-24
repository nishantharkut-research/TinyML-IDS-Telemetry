#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/.." && pwd)
compose=(docker compose -f "$repository_root/compose.yaml")

"${compose[@]}" up --detach --build --wait kafka hadoop spark app
python3 "$repository_root/scripts/verify-services.py"
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 \
    --create \
    --if-not-exists \
    --partitions 1 \
    --replication-factor 1 \
    --topic ids.telemetry.raw
topic_description=$("${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 \
    --describe \
    --topic ids.telemetry.raw)
if ! grep -Eq 'PartitionCount:[[:space:]]+1([[:space:]]|$)' <<<"$topic_description"; then
    printf 'Kafka topic ids.telemetry.raw must have exactly one partition\n' >&2
    exit 1
fi
if ! grep -Eq 'ReplicationFactor:[[:space:]]+1([[:space:]]|$)' <<<"$topic_description"; then
    printf 'Kafka topic ids.telemetry.raw must have replication factor one\n' >&2
    exit 1
fi
"${compose[@]}" exec -T hadoop hdfs dfs -mkdir -p \
    /tinyml-ids/input \
    /tinyml-ids/output \
    /tinyml-ids/tests
python3 "$repository_root/scripts/verify-services.py"
