# TinyML IDS Telemetry System Design

## Objective

Build a course-ready IoT and big-data management system that replays completed,
hardware-validated TinyML IDS telemetry through a fault-aware analytical
pipeline. The system demonstrates event streaming, distributed state,
watermarks, deduplication, columnar storage, Hadoop batch processing, query
serving, and data-retention trade-offs without repeating model training or
changing validated firmware.

## Source Evidence

The source is the sealed CuKD-XAI Wi-Fi HIL evidence for four board-model pairs:

| Board | Model | Full-stage rows |
| --- | --- | ---: |
| ESP32-C3 | Student A | 56,301 |
| ESP32-C3 | Student B | 56,301 |
| Arduino R4 WiFi | Student A | 56,301 |
| Arduino R4 WiFi | Student B | 56,301 |

The 225,204 rows are physical board-model executions over the same 56,301
model-record pairs. Each MCU row joins one-to-one with a fixed-point reference
row by `row_id`. The class order is Blackhole, Flooding, Grayhole, Normal, TDMA;
Normal has class identifier 3. Both preserved models use a final fixed-point
logit fractional width of 9, but scale remains lineage metadata rather than a
global assumption.

The evidence establishes controlled-LAN UDP replay of already extracted
17-feature WSN-DS records and exact MCU-to-fixed-reference prediction and logit
agreement. It does not establish live packet capture, on-device feature
extraction, transport security, natural event chronology, or deployment at
Internet scale.

## System Boundary

```text
Read-only CuKD-XAI evidence
          |
          v
Minimal verified local evidence bundle
          |
          v
Evidence audit and canonical snapshot
          |
          v
Deterministic analytics replay producer
          |
          v
Kafka delivery log
          |
          +----------------------+
          v                      v
Bronze delivery archive     Spark validation and state
                                 |
                          +------+------+
                          v             v
                    Silver Parquet   Quarantine
                          |
                          v
                  Gold finalization
                          |
             +------------+-------------+
             v                          v
      Retention views             SQL aggregates
             |                          |
             +------------+-------------+
                          v
                 Read-only dashboard

Canonical snapshot -> Hadoop Streaming oracle -> Spark/Hadoop reconciliation
```

Kafka is an educational analytics replay layer. It does not replace or emulate
the original stop-and-wait MCU UDP protocol.

## Data Layers

### Evidence snapshot

The importer first audits the source and atomically copies only the 79 required
artifacts while preserving their repository-relative paths and hashes. Normal
execution reads this local bundle and does not require the source checkout.
Neither import nor snapshot creation writes below either evidence root.

The adapter verifies source manifests and hashes, validates all four expected
lineages, joins MCU and reference CSVs, and writes canonical JSON Lines plus a
snapshot manifest outside the source repository. Snapshot generation fails on
missing rows, duplicate `row_id`, mismatched predictions, mismatched logits,
unknown classes, unknown fixed-point scale, or any source-hash discrepancy.

### Kafka delivery

The producer replays canonical events at a controlled rate. The Kafka key is
`lineage_id`, which preserves source row order within each physical session.
Replay modes cover normal pacing, accelerated pacing, deterministic duplicate
injection, controlled out-of-order delivery, and resume from a source offset.

### Bronze

Bronze is append-only and records every delivery with topic, partition, offset,
Kafka timestamp, key, replay metadata, and the original JSON payload. Delivery
duplicates remain visible.

### Silver and quarantine

Silver contains typed schema-valid events. Streaming deduplication is bounded by
the declared watermark, so it is not a global exactly-once claim. Invalid or
unsupported records are written to quarantine with machine-readable reasons.

### Gold

Gold is finalized after replay completion. It performs exact deduplication by
`source_event_id`, verifies the expected lineages and row counts, and publishes
run-level metrics atomically. A complete four-lineage run contains 225,204
unique source executions.

## Identity and Provenance

`source_event_id` is the SHA-256 digest of the canonical UTF-8 tuple:

```text
protocol_id|export_id|wireless_bundle_id|source_session_id|source_stage_id|row_id
```

`replay_run_id` identifies one analytics replay and is excluded from source
identity. `lineage_id` is a stable digest of lineage-level identifiers. Detailed
hashes live in a lineage dimension and are referenced from events.

The canonical event includes:

- schema and source identifiers;
- board and model identifiers;
- source row and original dataset row identifiers;
- synthetic replay event time and ingestion time;
- true and predicted class identifiers and names;
- fixed-point logits, fractional width, and normalized top-two margin;
- MCU preprocessing, inference, and total times;
- retry, timeout, ignored-datagram, request/response byte, and RTT fields;
- explicit prediction and logit equivalence flags.

## Retention Policies

### retain-all-v1

Retains every valid unique event.

### security-aware-static-v1

Retains:

1. every predicted attack;
2. every non-OK device response;
3. every retry, timeout, ignored datagram, or protocol anomaly;
4. every predicted-normal event below its model-specific normalized margin;
5. a deterministic sample of the remaining predicted-normal events.

The decision records policy identifier, version, reason, inclusion probability,
and inverse-probability weight. Online decisions never use `true_label`. The
runtime guarantee is preservation of predicted attack alerts. Offline labeled
evaluation separately reports retention of actual attacks, including attacks
that the model predicted as Normal.

## Big-Data Concepts Demonstrated

- Kafka topics, partitions, keys, offsets, ordering, and at-least-once delivery;
- synthetic event time versus ingestion and processing time;
- synthetic event time, watermarks, bounded disorder, and stateful deduplication;
- checkpoints, restart recovery, backpressure, and idempotent finalization;
- HDFS raw and curated zones;
- Parquet schema, compression, partition pruning, and predicate pushdown;
- Hadoop Streaming MapReduce as an independent batch correctness oracle;
- Spark SQL serving tables and a thin read-only dashboard;
- completeness, uniqueness, validity, and timeliness data-quality dimensions;
- volume, velocity, variety, veracity, and value using one traceable dataset.

## Batch Oracle

The Hadoop mapper/reducer independently computes exact counts and integer sums
from the canonical snapshot: total and unique records; duplicate count; counts
by board, model, true class, and predicted class; predicted alerts; retries;
timeouts; latency sums; and retention counts by policy and reason. Spark and
Hadoop counts and sums must match exactly. Means are derived from exact sums and
counts. Approximate percentiles are compared only with declared tolerances.

## Course Experiment Matrix

| Experiment | Evidence produced |
| --- | --- |
| Normal replay | End-to-end correctness and baseline throughput |
| Accelerated replay | Throughput, queue growth, and backpressure |
| Duplicate injection | Bronze duplicates and stable Gold results |
| Out-of-order replay | Watermark-bounded disorder with no scheduled late loss |
| Restart recovery | Offset/checkpoint recovery and idempotent finalization |
| Retain all | Storage and query reference |
| Security-aware retention | Alert preservation and storage/query trade-off |
| Batch reconciliation | Exact Spark/Hadoop count and sum agreement |

## Technology Baseline

- Python 3.11+ for contracts, adapters, replay, policies, and Hadoop scripts;
- Apache Kafka 4.1.2 in single-node KRaft mode for the local course stack;
- Apache Spark 4.1.3 with Structured Streaming and the matching Kafka connector;
- Apache Hadoop 3.5.0 for HDFS and Hadoop Streaming;
- Parquet for curated storage;
- Streamlit only as a presentation layer over finalized aggregates.

Pinned container versions are local reproducibility choices, not production
deployment recommendations. The official Apache images are used where available.

## Explicit Non-Goals

- model retraining, hyperparameter search, or new ML baselines;
- firmware changes or new hardware measurements;
- live radio capture or WSN feature extraction;
- SHAP, concept drift, cross-dataset validation, or adaptive retention;
- transport encryption or authentication claims;
- Cassandra, MongoDB, Hive, Flink, Kubernetes, Airflow, or cloud deployment;
- formal query-preservation guarantees in the course version.

## Research Extension

A later study can introduce workload-aware retention, formal query-error bounds,
additional retention baselines, adaptive policies, larger device populations,
natural telemetry streams, and measured energy/network cost. Those additions
must remain outside the course baseline until the baseline is complete.

## Acceptance Criteria

1. The source repository is never modified and output-under-source is rejected.
   The imported bundle re-audits independently before analytics begin.
2. Four expected lineages and their source hashes validate.
3. Every lineage has 56,301 unique contiguous rows.
4. MCU/reference joins, predictions, and all five logits agree exactly.
5. Repeated replay produces identical source identifiers.
6. Duplicate delivery changes Bronze counts but not finalized Gold counts.
7. Invalid events enter quarantine with deterministic reasons.
8. Missing class mapping or logit scale fails closed.
9. Every predicted attack is retained by the security-aware policy.
10. Changing true labels does not change online retention decisions.
11. Hadoop and Spark exact aggregates reconcile.
12. Full-evidence results preserve the repeated-execution interpretation.

## Primary References

- Apache Kafka quick start: https://kafka.apache.org/quickstart/
- Spark Structured Streaming: https://spark.apache.org/docs/4.1.3/streaming/
- Spark Kafka integration: https://spark.apache.org/docs/4.1.3/streaming/structured-streaming-kafka-integration.html
- Hadoop Streaming: https://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html
