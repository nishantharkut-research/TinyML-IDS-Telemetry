# Course Experiment Protocol

## Purpose

This protocol evaluates the data system built over sealed TinyML IDS hardware
telemetry. It does not evaluate a new classifier. The experiments test whether
the pipeline preserves records, handles controlled delivery faults, produces
reproducible aggregates, and supports a measurable storage trade-off.

## Evidence Units

The source contains four board-model execution lineages. Each lineage has
56,301 physical executions over the same predictive sequence.

| Quantity | Full evidence value | Interpretation |
| --- | ---: | --- |
| Execution lineages | 4 | Two models on two MCU boards |
| Hardware executions | 225,204 | Physical board-model executions |
| Unique predictive rows | 56,301 | Distinct rows in the shared test sequence |
| Replay deliveries | Run dependent | Kafka deliveries after controlled fault injection |

The 225,204 executions are not independent predictive samples. Replay
deliveries can exceed 225,204 when an experiment injects duplicates. Event
timestamps are deterministic analytics replay time. They are not natural
packet-capture timestamps.

## Fixed Controls

Every comparison uses the same sealed evidence bundle, canonical event schema,
source-event identities, lineage keys, Spark transformations, Gold finalization
rules, and Hadoop aggregate definitions. The following values must be recorded
with each result:

- Git commit and container image identifiers
- evidence bundle identifier and canonical snapshot hash
- replay run identifier and replay seed
- Kafka topic, partition count, and producer settings
- replay rate and fault schedule
- Spark checkpoint path, watermark, and trigger settings
- retention policy identifier and configuration hash
- host CPU, memory, storage, operating system, and Docker version
- trial start time, end time, and finalization status

The local course baseline uses one Kafka partition to make ordering and fault
behavior easy to inspect. This is a correctness baseline, not a maximum-scale
Kafka configuration.

## Measurements

### Correctness

- Bronze delivery count
- valid Silver count
- quarantine count by reason
- unique finalized Gold source-event count
- missing and duplicate source-event counts
- exact Spark-Hadoop differences by field

### Performance

- producer delivery rate in deliveries per second
- finalized analytics rate in unique source events per second
- replay wall-clock duration
- finalization wall-clock duration
- maximum observed Kafka lag when available

The production driver publishes these run-bound measurements to
`artifacts/course/runs/<run-id>/pipeline_execution.json`. The document records
actual phase durations, replay and finalized rates, source and delivery counts,
the replay-recovery state, and byte totals for streaming and finalized output
trees. The course report consumes this file directly. Rates must not be copied
from console output or inferred from configured replay targets.

For restart recovery, scheduled and acknowledged counts describe only the
resumed replay tail. Snapshot and finalized counts describe the complete run.
These quantities remain separate in the execution report and must not be
substituted for one another.

Throughput measures the analytics replay pipeline. It does not measure physical
MCU transmission throughput. MCU timing and Wi-Fi round-trip fields are frozen
source observations and are reported separately from pipeline wall-clock time.

### Storage and Retention

- bytes in Bronze, Silver, Gold, and each retained Parquet view
- retained hardware-execution count and rate
- retained predicted-attack count and rate
- offline retained actual-attack count and rate
- count by policy decision reason

`canonical_gold` is the retain-all storage reference. The separately
materialized `retained_security_aware` Parquet view is the measured
security-aware output. Storage reduction is computed from their recorded byte
totals only after both outputs are finalized.

Online retention decisions cannot use the true label. Actual-attack retention
is an offline labeled evaluation only.

## Experiment Matrix

### E1. Normal replay

Replay the canonical snapshot once without injected duplicates or reordering.
The expected result is one Bronze delivery and one finalized Gold event per
source event, zero quarantine records, and exact Spark-Hadoop agreement. This
run establishes the correctness and nominal throughput reference.

### E2. Accelerated replay

Replay the same snapshot at increasing configured rates while holding the
schema, topic, partition key, transformations, and storage paths constant.
Record achieved producer rate, finalized analytics rate, wall-clock duration,
and Kafka lag. Stop increasing the rate when lag no longer returns to zero or a
resource limit is reached. Report the tested range rather than claiming a
universal maximum.

### E3. Duplicate delivery

Inject duplicates with a fixed replay seed and declared probability. Bronze
must preserve every delivery. Finalized Gold must still contain exactly one row
per source-event identifier. Compare Bronze amplification, Silver bounded
deduplication, and Gold exact deduplication.

### E4. Out-of-order delivery

Apply a deterministic reordering window while keeping the event payload and
source identity unchanged. The declared watermark must strictly exceed the
maximum skew introduced by that schedule. Verify bounded streaming
deduplication and complete final Gold output with no schedule-induced loss. A
watermark that is too short must be rejected before Spark starts.

### E5. Restart recovery

Interrupt the streaming application after a recorded Kafka offset, preserve
Kafka and checkpoint volumes, then restart the same run. Verify offset recovery,
checkpoint reuse, no loss in finalized Gold, and idempotent final publication.
The interruption point is part of the result record.

### E6. Retain all

Apply `retain-all-v1` to the finalized dataset. Every valid event must be
retained. This view is the storage and query reference for policy comparison.

### E7. Security-aware retention

Apply `security-aware-static-v1` to the same finalized dataset. Verify that all
predicted attacks and all declared protocol anomalies are retained. Report
storage reduction, decision reasons, and offline actual-attack retention. The
policy is compared with retain-all on identical source events.

### E8. Independent batch reconciliation

Run Hadoop Streaming over the canonical evidence and compare its exact counts
and integer sums with Spark's finalized aggregate document. The experiment
passes only when every exact field matches and the difference list is empty.
This is an independent correctness check because the mapper and reducer do not
reuse Spark transformations.

## Trial Policy

Deterministic correctness experiments must pass first on the small fixture and
then on the full four-lineage evidence. Performance measurements use one warm-up
run followed by three measured runs with unchanged settings. Report the median,
minimum, and maximum. Storage and exact aggregate results are deterministic and
need one successful full-evidence publication per policy configuration.

Failed and interrupted runs remain recorded with their reason. They are not
silently replaced by successful runs.

## Interpretation Limits

The experiments demonstrate a reproducible single-host implementation of
distributed data-management concepts. They do not establish Internet-scale
capacity, natural traffic chronology, live packet capture, cross-site fault
tolerance, or physical device throughput. Larger synthetic replay counts test
pipeline scaling behavior, but they do not create new predictive evidence.
