# IoT and Big-Data Management Concepts

## Project Role

The prior TinyML work established that compact intrusion-detection models could
execute on IoT boards. This project starts after inference. It manages the
resulting predictions, timing records, retries, device status, and protocol
metadata as an auditable data stream.

The Big Data Management role is therefore not defined by the number of columns.
It is defined by the number of devices, their event rate, the duration of data
collection, repeated delivery, event ordering, distributed state, storage, and
the need to verify analytical results.

## Concept Map

| Taught concept | System implementation | Evidence shown in the course project |
| --- | --- | --- |
| IoT telemetry | Frozen ESP32-C3 and Arduino R4 WiFi execution records | Board, model, prediction, logits, MCU timing, Wi-Fi timing, retries, and status |
| Kafka topic | Versioned telemetry topic | Reproducible producer and consumer boundary |
| Partition key | Stable `lineage_id` | Ordered source rows inside one board-model lineage |
| Offset | Kafka delivery metadata in Bronze | Replay position, restart point, and delivery trace |
| At-least-once delivery | Controlled duplicate injection and restart | Bronze preserves deliveries while Gold removes duplicate source identities |
| Event time | Deterministic synthetic replay timestamp | Repeatable windows independent of host clock variation |
| Processing time | Spark execution clock | Pipeline timing separated from source MCU timing |
| Watermark | Bounded late-data policy in Spark | Declared handling of reordered and late records |
| Stateful deduplication | Watermark-bounded Silver deduplication | Duplicate control with finite streaming state |
| Bronze layer | Append-only Kafka delivery archive | Topic, partition, offset, headers, timestamp, and original payload |
| Silver layer | Typed valid events plus quarantine | Schema validation, deterministic reasons, and data-quality counts |
| Gold layer | Exact source-event deduplication and finalization | Stable unique records and atomic aggregate publication |
| HDFS | Distributed file-system storage for oracle inputs and outputs | Persistent paths, restart survival, and Hadoop job exchange |
| Parquet | Columnar Silver, Gold, and retention views | Typed schema, compression, projection, and predicate pushdown |
| Spark SQL | Aggregate builders and analytical queries | Counts, timing sums, class distribution, failures, and policy outcomes |
| Hadoop MapReduce | Independent mapper and reducer | Exact count and integer-sum oracle |
| Reconciliation | Field-level Spark-Hadoop comparison | Empty difference list required for a passing run |
| NoSQL knowledge | Event keys, immutable records, partitioning, and denormalized views | Design choices that avoid transactional row updates in the stream path |
| Retention management | Versioned retain-all and security-aware policies | Storage reduction and alert-preservation trade-off |

## Data Quality

| Dimension | Check |
| --- | --- |
| Completeness | Expected four lineages and contiguous source rows are present |
| Uniqueness | Stable source-event identity detects repeated deliveries |
| Validity | JSON Schema, class mapping, logits, scale, and typed fields validate |
| Consistency | MCU and fixed-reference predictions and logits agree before replay |
| Timeliness | Watermark and late-event outcomes are recorded under synthetic event time |
| Verifiability | Spark exact aggregates reconcile with an independent Hadoop oracle |

## The Five Vs

### Volume

Volume grows approximately as:

```text
device count x events per device per second x collection duration
```

The sealed course evidence has 225,204 hardware executions. A million-record
demonstration repeats canonical events with new replay-delivery identities. It
tests the data pipeline only and does not claim a million independent IDS
samples.

### Velocity

Kafka replay controls delivery rate. Spark reports achieved processing rate,
lag, and finalization time under normal and accelerated runs.

### Variety

The event contract combines categorical security outputs, fixed-point logits,
timing fields, byte counts, retry counters, status values, and provenance.

### Veracity

Hash-verified evidence, schema validation, quarantine, immutable identity,
exact finalization, and Spark-Hadoop reconciliation protect data correctness.

### Value

The system provides operational monitoring, security-event summaries,
reproducible reports, and a measurable storage-versus-retention comparison.

## Scalability Path

The course baseline deliberately uses one local Kafka partition and one host so
that every correctness property can be inspected. The architecture scales along
clear boundaries:

1. Add device lineages and partition Kafka by a stable device or lineage key.
2. Increase Kafka partitions and Spark executors together.
3. Place Parquet data in distributed HDFS storage partitioned by run and time.
4. Keep deduplication state bounded with declared watermarks.
5. Run independent Hadoop aggregation over the same canonical input.
6. Scale dashboard access through finalized aggregate tables, not raw telemetry.

Adding Docker Compose improves reproducibility, but it does not prove scale.
Kubernetes is unnecessary for the course baseline because it would add cluster
operations without changing the data-management questions being evaluated.

## Claim Boundary

The project demonstrates a complete and reproducible data-management pattern
on controlled telemetry. It does not claim production deployment, live network
capture, new machine-learning accuracy, natural event chronology, or measured
Internet-scale capacity.
