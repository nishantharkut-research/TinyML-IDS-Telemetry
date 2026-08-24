#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/.." && pwd)
compose=(docker compose -f "$repository_root/compose.yaml")

"$repository_root/scripts/bootstrap.sh"
"${compose[@]}" exec -T \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 \
    -e TINYML_IDS_REQUIRE_KAFKA_INTEGRATION=1 \
    app python3 -m pytest tests/integration/test_kafka_replay.py -q
"${compose[@]}" exec -T \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 \
    -e KAFKA_TOPIC=ids.telemetry.fixture \
    -e TINYML_IDS_REQUIRE_SPARK_INTEGRATION=1 \
    spark python3 -m pytest tests/integration/test_spark_pipeline.py -q
"${compose[@]}" exec -T \
    -e HADOOP_HOME=/opt/hadoop \
    -e TINYML_IDS_HDFS_TEST_ROOT=/tinyml-ids/tests \
    -e TINYML_IDS_RUN_HADOOP_INTEGRATION=1 \
    hadoop python3 -m pytest tests/integration/test_hadoop_streaming.py -q
python3 "$repository_root/scripts/verify-services.py"
