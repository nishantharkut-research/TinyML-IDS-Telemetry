# Post-Course Research Extension

## Boundary

The current repository is a course system. Its present contribution is a
reproducible IoT telemetry pipeline that demonstrates Kafka, Spark, HDFS,
Hadoop, Parquet, SQL, data quality, retention, and independent reconciliation.

Research extension work starts after the semester. The current implementation
does not claim a new distributed algorithm, Internet-scale capacity, or a
publication result.

## Candidate Research Question

The strongest extension is to study whether cross-engine verification and
security-aware retention can provide measurable correctness and storage
benefits for TinyML IDS telemetry under realistic delivery faults.

The paper-level question would be:

> Which telemetry faults are detected by an independent batch oracle that are
> missed by single-engine stream checks, and how much backend storage can be
> reduced while preserving declared security and operational queries?

This question builds on the course system without requiring new model training.

## Required Extension

### Fault-Detection Matrix

Inject controlled faults at distinct boundaries:

- duplicate Kafka delivery
- delayed and out-of-order delivery
- malformed or schema-incompatible events
- dropped replay segments
- replay restart at recorded offsets
- conflicting payloads under one source identity
- incomplete Spark publication
- altered aggregate or provenance files

Measure which controls detect each fault, where it is quarantined, and whether
the independent Hadoop oracle identifies an otherwise silent discrepancy.

### Scale Evaluation

Scale device lineages, Kafka partitions, event rates, collection duration, and
Spark executors independently. Repetition for scale testing must use new
delivery identities and must not be described as new IDS samples.

Report achieved rate, lag recovery, state size, finalization time, HDFS and
Parquet bytes, CPU, memory, and failure recovery. Compare against a single-node
baseline before introducing a cluster.

### Retention Evaluation

Compare retain-all, fixed sampling, time-window retention, and
security-aware retention on identical finalized events. Predeclare the queries
that each policy must preserve. Report storage bytes, query-result error,
predicted-alert retention, operational-anomaly retention, and offline
true-attack retention separately.

### Baselines

At minimum, compare:

- Spark-only validation without Hadoop reconciliation
- Spark plus independent Hadoop oracle
- JSON or CSV storage versus Parquet
- retain-all versus standard sampling and security-aware retention
- one partition versus scaled partition and executor settings

## Evidence to Preserve Now

Every course run should retain a compact manifest with the Git commit,
container identities, snapshot hash, run options, Kafka topic and partition
count, Spark checkpoints, policy hash, hardware specification, exact output
hashes, and reconciliation result. Controlled failures must be retained rather
than replaced by successful reruns.

These records reduce later research work. They do not turn the course run into
a paper result by themselves.

## Deferred Infrastructure

Docker Compose is sufficient for the correctness baseline. A multi-node Spark
and Kafka deployment, Kubernetes, object storage, and cloud execution should be
added only when a declared scale experiment requires them. Infrastructure is a
means of testing the research question, not the contribution by itself.
