# Repository Instructions

## Purpose

This repository builds a reproducible big-data analytics pipeline over completed
TinyML intrusion-detection telemetry. It does not train models, modify firmware,
or replace the physical hardware-in-the-loop protocol.

## Evidence Boundary

- Treat the CuKD-XAI repository as read-only source evidence.
- Copy only catalog-whitelisted, hash-verified artifacts into the local evidence
  bundle. "Move" never means deleting or relocating the source artifacts.
- After import, use `TINYML_IDS_EVIDENCE_ROOT`; normal analytics must not depend
  on the CuKD-XAI checkout remaining available.
- Resolve the one-time source through `CUKD_XAI_ROOT`; never persist one
  developer's absolute path in bundle manifests or generated reports.
- Never write generated data, checkpoints, caches, or reports below
  `CUKD_XAI_ROOT`.
- Verify source hashes and lineage metadata before adapting records.
- Preserve the interpretation of 225,204 records: four physical board-model
  executions over the same 56,301 test records, not 225,204 independent samples.
- Label event time as synthetic replay time. Do not claim natural chronology,
  concept drift, live packet capture, live feature extraction, or Internet-scale
  operation.
- Describe storage reduction as gateway-to-backend reduction. The source
  MCU-to-gateway Wi-Fi traffic is already complete and is not reduced here.

## Delivery Semantics

- Use deterministic `source_event_id` values for source identity.
- Keep `replay_run_id` separate from source identity.
- Bronze data preserves every Kafka delivery, including duplicates.
- Silver data is typed, schema-valid, and bounded-deduplicated for streaming.
- Gold data is finalized with exact end-of-run deduplication and reconciliation.
- State the guarantee as at-least-once delivery with deterministic identities,
  deduplication, and idempotent analytical outputs. Do not claim universal
  exactly-once delivery.

## Retention Rules

- Retention decisions are derived records. Never mutate or delete canonical
  evidence.
- `retain-all-v1` must retain every valid source event.
- `security-aware-static-v1` must retain every predicted attack and every
  operational anomaly.
- Online retention must not use `true_label`.
- Report true-attack retention separately as offline labeled evaluation.
- Margin thresholds are model-specific and use normalized fixed-point logits.

## Engineering Rules

- Use Python 3.11 or newer. Keep the core contract and evidence adapter free of
  Spark and Kafka imports.
- Follow test-driven development for behavior changes: write the failing test,
  observe the expected failure, implement the smallest passing change, then run
  the broader suite.
- Prefer standard-library data structures at contract boundaries.
- Validate explicitly and fail closed on missing hashes, class mappings, logit
  scales, row gaps, duplicate source rows, prediction mismatches, or logit
  mismatches.
- Keep files focused. Avoid framework wrappers and speculative abstractions.
- Never commit credentials, source datasets, generated Parquet, Kafka data,
  Spark checkpoints, HDFS volumes, or local evidence paths.
- External agents may review read-only or work on disjoint files. A primary
  maintainer must verify every result before integration.
- Do not fabricate or pre-announce a citation for unpublished companion work.
  Add the article citation only after a stable bibliographic record exists.

## Verification

Before merging:

1. Run unit, contract, property, and local batch tests.
2. Run static checks and type checks.
3. Run the fixture end-to-end pipeline.
4. When Docker is available, run Kafka, Spark, and Hadoop integration tests.
5. Run the full four-lineage evidence audit separately from normal unit tests.
6. Confirm the CuKD-XAI source files used by the run still match their recorded
   SHA-256 values.

## Claims

Documentation and dashboards must distinguish:

- source evidence from replay deliveries;
- predictive samples from repeated hardware executions;
- MCU compute time from host-observed UDP round-trip time;
- predicted-attack preservation from true-attack retention;
- replay-pipeline throughput from physical device throughput;
- fixed-point logit scale from model-independent confidence.
