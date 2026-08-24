# Course Baseline Status

**Verification date:** 24 August 2026
**Branch:** `feat/course-foundation`

## Status

The software foundation and distributed fixture pipeline are verified. The
full measured course experiment matrix has not been executed on this laptop.
It remains scheduled for the designated execution machine after the code and
evidence snapshot are transferred.

This document separates completed verification from pending measurements.

## Verified Evidence

| Check | Verified value |
| --- | ---: |
| Evidence bundle ID | `dab62c721fe05fc8d3e763284ff84d356cb59d7895d4e4a08b7865cc6b99a991` |
| Canonical snapshot ID | `d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff` |
| Hardware execution lineages | 4 |
| Hardware executions audited | 225,204 |
| Predictive rows per lineage | 56,301 |
| Prediction mismatches | 0 |
| Fixed-point logit mismatches | 0 |

The 225,204 audited rows are four physical execution lineages over the same
56,301-row predictive sequence.

## Verified Software Baseline

| Layer | Completed verification |
| --- | --- |
| Host checks | 601 passed, 5 skipped, 10 deselected |
| Kafka fixture | 3 integration tests passed |
| Spark fixture | 4 integration tests passed |
| Hadoop fixture | 2 integration tests passed |
| Restart persistence | Kafka, HDFS, Spark Ivy, and artifact named volumes retained state across service restart |
| Dashboard | Desktop and mobile layouts rendered with zero browser-console errors or warnings |
| Spark driver | Replay, query drain, cleanup, exact finalization, execution measurement, and restart-receipt paths covered |
| Course report | Finalized-run hash, measured execution record, and Spark-Hadoop reconciliation required before publication |

The host matrix excludes service-gated integration tests and the opt-in full
evidence regression. Those checks are recorded separately above and in the
branch history.

## Pending Measured Results

| Experiment result | Status |
| --- | --- |
| Normal full-evidence replay throughput | Pending designated-machine run |
| Accelerated replay rate sweep | Pending designated-machine run |
| Duplicate-delivery amplification and exact recovery | Pending designated-machine run |
| Out-of-order and watermark behavior | Pending designated-machine run |
| Controlled restart recovery | Pending designated-machine run |
| Retain-all Parquet bytes | Pending designated-machine run |
| Security-aware retained bytes and event counts | Pending designated-machine run |
| Full Spark-Hadoop exact reconciliation | Pending designated-machine run |

No throughput, scale, or storage-saving number is inferred from fixture tests.
The course report must be generated from a finalized full-evidence run and a
matching Hadoop reconciliation record.

## Acceptance Rule

The measured baseline can replace the pending entries only when all of the
following conditions hold:

1. the run uses the frozen canonical snapshot and a fresh one-partition topic
2. the replay receipt matches the declared run options
3. Gold contains exactly one finalized event per expected source identity
4. every output has a success marker and matches the finalized manifest
5. Spark and Hadoop exact fields reconcile with no differences
6. performance trials follow one warm-up plus three unchanged measured runs
7. reported units distinguish executions, predictive rows, and replay deliveries
