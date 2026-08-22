# TinyML IDS Telemetry Course System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible Kafka, Spark, HDFS, Hadoop Streaming, and dashboard pipeline over immutable CuKD-XAI Wi-Fi HIL telemetry.

**Architecture:** A pure-Python evidence adapter creates canonical events from four verified hardware lineages. Kafka replays those events into Bronze delivery storage, Spark produces validated Silver and exact finalized Gold datasets, versioned policies create retained views, and Hadoop Streaming independently verifies exact aggregates.

**Tech Stack:** Python 3.11+, pytest, JSON Schema, Kafka 4.1.2, Spark 4.1.3, Hadoop 3.5.0, Parquet, Docker Compose, Streamlit.

---

## File Map

- `src/tinyml_ids_telemetry/contracts/`: event, lineage, validation, identity.
- `src/tinyml_ids_telemetry/evidence/`: read-only CuKD-XAI audit and adapter.
- `src/tinyml_ids_telemetry/replay/`: deterministic clocks, schedules, Kafka delivery.
- `src/tinyml_ids_telemetry/policies/`: versioned retention decisions.
- `src/tinyml_ids_telemetry/quality/`: data-quality and exact aggregate functions.
- `src/tinyml_ids_telemetry/cli/`: thin command-line entry points.
- `jobs/spark/`: Bronze, Silver, Gold, and aggregate jobs.
- `jobs/hadoop/`: independent mapper, reducer, and reconciliation code.
- `dashboard/`: read-only presentation layer.
- `schemas/`: versioned JSON Schema contracts.
- `configs/`: portable evidence catalog and policy configuration.
- `infra/`: local reproducible service definitions.
- `tests/`: unit, contract, property, integration, and full-evidence checks.

### Task 1: Repository Foundation

**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Create: `pyproject.toml`
- Create: `README.md`
- Create: `CONTRIBUTING.md`
- Create: `CITATION.cff`
- Create: `src/tinyml_ids_telemetry/__init__.py`
- Create: `tests/conftest.py`

- [x] Write a metadata test that imports the package and checks its version.
- [x] Run `py -3.11 -m pytest tests/unit/test_package.py -v` and observe the import failure.
- [x] Add the minimal package metadata and module.
- [x] Create `.venv` with Python 3.11 and install `.[dev]`.
- [x] Run `python -m pytest -q`, `python -m ruff check .`, and `python -m mypy src`.
- [x] Commit the repository foundation.

### Task 2: Canonical Contracts and JSON Schema

**Files:**
- Create: `schemas/telemetry-event-v1.schema.json`
- Create: `src/tinyml_ids_telemetry/contracts/event.py`
- Create: `src/tinyml_ids_telemetry/contracts/identity.py`
- Create: `src/tinyml_ids_telemetry/contracts/lineage.py`
- Test: `tests/unit/contracts/test_identity.py`
- Test: `tests/contract/test_event_schema.py`

- [x] Write failing tests for canonical SHA-256 source and lineage identifiers.
- [x] Run the identity tests and confirm missing-module failures.
- [x] Implement canonical tuple encoding and identifier functions.
- [x] Write failing schema tests for valid events, malformed logits, unsupported versions, and invalid class identifiers.
- [x] Run the schema tests and confirm validation failures.
- [x] Implement immutable event/lineage dataclasses and JSON serialization.
- [x] Add the versioned JSON Schema and prove dataclass output validates against it.
- [x] Run contract, static, and type checks; commit the contract layer.

### Task 3: Immutable Evidence Audit and Adapter

**Files:**
- Create: `configs/evidence-catalog.toml`
- Create: `src/tinyml_ids_telemetry/evidence/catalog.py`
- Create: `src/tinyml_ids_telemetry/evidence/hashes.py`
- Create: `src/tinyml_ids_telemetry/evidence/audit.py`
- Create: `src/tinyml_ids_telemetry/evidence/adapter.py`
- Create: `src/tinyml_ids_telemetry/evidence/bundle.py`
- Create: `src/tinyml_ids_telemetry/evidence/snapshot.py`
- Create: `src/tinyml_ids_telemetry/cli/evidence.py`
- Test: `tests/helpers/evidence_factory.py`
- Test: `tests/unit/evidence/test_catalog.py`
- Test: `tests/unit/evidence/test_audit.py`
- Test: `tests/unit/evidence/test_adapter.py`
- Test: `tests/full_evidence/test_cukd_xai.py`

- [x] Build a provenance-complete fixture with MCU, reference, compile, completion, export, and preprocessing artifacts.
- [x] Write failing tests for root resolution and rejecting output paths inside the evidence root.
- [x] Implement portable TOML catalog resolution through `TINYML_IDS_EVIDENCE_ROOT` with a `CUKD_XAI_ROOT` import fallback.
- [x] Write failing tests for hash mismatch, row gaps, duplicate rows, prediction mismatch, logit mismatch, unknown class mapping, and missing output scale.
- [x] Implement streaming CSV audit and one-to-one joins without loading all source rows into memory.
- [x] Write failing tests for canonical event adaptation and normalized top-two margin.
- [x] Implement an atomic minimal evidence-bundle import and an atomic JSONL snapshot.
- [x] Add an opt-in test for all four real lineages and 225,204 unique source events.
- [x] Rebuild the same frozen snapshot from both the read-only source and imported bundle.
- [x] Run fixture, static, type, and full-evidence checks; commit the evidence layer.

### Task 4: Retention Policies and Exact Aggregates

**Files:**
- Create: `configs/retention-policies.toml`
- Create: `src/tinyml_ids_telemetry/policies/base.py`
- Create: `src/tinyml_ids_telemetry/policies/retain_all.py`
- Create: `src/tinyml_ids_telemetry/policies/security_aware.py`
- Create: `src/tinyml_ids_telemetry/quality/aggregates.py`
- Create: `src/tinyml_ids_telemetry/quality/retention.py`
- Test: `tests/unit/policies/test_retain_all.py`
- Test: `tests/unit/policies/test_security_aware.py`
- Test: `tests/property/test_policy_invariants.py`

- [x] Write failing tests proving retain-all never drops a valid event.
- [x] Implement `retain-all-v1`.
- [x] Write failing tests for predicted attacks, protocol anomalies, low margins, and deterministic normal sampling.
- [x] Implement `security-aware-static-v1` with reason, probability, and inverse weight.
- [x] Write a property test that mutates true labels without changing decisions.
- [x] Write failing tests for exact integer aggregate merge behavior.
- [x] Implement mergeable count/sum aggregates and offline true-attack retention evaluation.
- [x] Run policy/property tests and commit the policy layer.

### Task 5: Deterministic Replay and Kafka Delivery

**Files:**
- Create: `src/tinyml_ids_telemetry/replay/clock.py`
- Create: `src/tinyml_ids_telemetry/replay/schedule.py`
- Create: `src/tinyml_ids_telemetry/replay/sink.py`
- Create: `src/tinyml_ids_telemetry/replay/kafka.py`
- Create: `src/tinyml_ids_telemetry/cli/replay.py`
- Test: `tests/unit/replay/test_clock.py`
- Test: `tests/unit/replay/test_schedule.py`
- Test: `tests/contract/test_kafka_record.py`
- Test: `tests/integration/test_kafka_replay.py`

- [ ] Write failing tests for deterministic synthetic time, duplicate injection, out-of-order delivery, and resume offsets.
- [ ] Implement a lazy replay schedule that does not mutate canonical events.
- [ ] Define a sink protocol and a memory sink for deterministic tests.
- [ ] Write failing Kafka serialization tests for key, headers, and payload.
- [ ] Implement an optional Kafka sink with delivery callbacks and flush/error handling.
- [ ] Add the CLI and dry-run JSONL sink.
- [ ] Run unit tests, then the Docker-marked Kafka integration test; commit replay support.

### Task 6: Spark Bronze, Silver, Gold, and Queries

**Files:**
- Create: `jobs/spark/common.py`
- Create: `jobs/spark/stream_telemetry.py`
- Create: `jobs/spark/finalize_run.py`
- Create: `jobs/spark/build_aggregates.py`
- Create: `queries/operational_metrics.sql`
- Create: `queries/retention_metrics.sql`
- Test: `tests/unit/jobs/test_spark_transformations.py`
- Test: `tests/integration/test_spark_pipeline.py`

- [ ] Write local Spark tests for JSON parsing, quarantine reasons, Kafka metadata preservation, watermark-bounded deduplication, and policy decision output.
- [ ] Implement pure DataFrame transformations separately from job startup.
- [ ] Implement Bronze append, Silver/quarantine Parquet, and checkpointed queries.
- [ ] Write failing finalization tests for exact source-event deduplication and expected-lineage checks.
- [ ] Implement atomic Gold finalization and finalized-run manifest creation.
- [ ] Add Spark SQL aggregate builders and documented SQL queries.
- [ ] Run Spark local tests and the Kafka-to-Parquet integration test; commit distributed processing.

### Task 7: HDFS and Hadoop Streaming Oracle

**Files:**
- Create: `jobs/hadoop/mapper.py`
- Create: `jobs/hadoop/reducer.py`
- Create: `jobs/hadoop/reconcile.py`
- Create: `scripts/run-hadoop-oracle.sh`
- Test: `tests/unit/jobs/test_hadoop_mapper.py`
- Test: `tests/unit/jobs/test_hadoop_reducer.py`
- Test: `tests/unit/jobs/test_reconciliation.py`
- Test: `tests/integration/test_hadoop_streaming.py`

- [ ] Write failing stdin/stdout tests for exact mapper keys and integer partials.
- [ ] Implement a pure standard-library mapper.
- [ ] Write failing reducer tests for deterministic aggregation and ordering.
- [ ] Implement a pure standard-library reducer.
- [ ] Write failing reconciliation tests that identify field-level mismatches.
- [ ] Implement Spark/Hadoop comparison with exact count/sum equality and declared percentile tolerances.
- [ ] Add an HDFS runner using the official Hadoop Streaming JAR.
- [ ] Run local pipeline and Docker Hadoop integration tests; commit the batch oracle.

### Task 8: Local Infrastructure and Operations

**Files:**
- Create: `compose.yaml`
- Create: `infra/spark/Dockerfile`
- Create: `infra/hadoop/`
- Create: `scripts/bootstrap.ps1`
- Create: `scripts/bootstrap.sh`
- Create: `scripts/run-fixture-e2e.ps1`
- Create: `scripts/run-fixture-e2e.sh`
- Create: `scripts/run-full-evidence.ps1`
- Create: `scripts/verify-services.py`
- Test: `tests/contract/test_compose_config.py`

- [ ] Write a contract test for pinned images, health checks, named volumes, and credential-free configuration.
- [ ] Add single-node KRaft Kafka, Spark, HDFS/MapReduce, and application service definitions.
- [ ] Add idempotent topic/HDFS bootstrap scripts.
- [ ] Add cross-platform fixture and full-evidence run scripts with fail-fast checks.
- [ ] Validate Compose configuration, health checks, restart behavior, and volume isolation.
- [ ] Commit the local platform.

### Task 9: Dashboard and Course Experiment Reporting

**Files:**
- Create: `dashboard/app.py`
- Create: `src/tinyml_ids_telemetry/quality/report.py`
- Create: `docs/experiment-protocol.md`
- Create: `docs/course-concepts.md`
- Test: `tests/unit/dashboard/test_data_access.py`
- Test: `tests/unit/quality/test_report.py`

- [ ] Write failing tests for loading only finalized aggregates and rejecting incomplete runs.
- [ ] Implement read-only dashboard data access and compact views for throughput, quality, latency, classes, failures, retention, and reconciliation.
- [ ] Write failing report tests for repeated-execution wording and claim boundaries.
- [ ] Implement machine-readable and Markdown experiment reports.
- [ ] Document the eight course experiments, units, controlled variables, and interpretation limits.
- [ ] Map each feature to the taught big-data concepts.
- [ ] Run dashboard/report tests and commit presentation support.

### Task 10: End-to-End Verification and Independent Review

**Files:**
- Create: `docs/reproducibility.md`
- Create: `docs/results/course-baseline.md`
- Create: `docs/research-extension.md`
- Create: `.github/workflows/ci.yml`
- Modify: `README.md`

- [ ] Run formatting, linting, typing, unit, contract, property, and fixture E2E tests.
- [ ] Start Docker and run Kafka, Spark, HDFS, Hadoop, restart-recovery, duplicate, and out-of-order integration tests.
- [ ] Run the four-lineage evidence audit and full 225,204-record replay.
- [ ] Reconcile Spark and Hadoop exact aggregates.
- [ ] Record throughput and storage results without predictive-sample or network-latency overclaims.
- [ ] Have independent agents review evidence boundaries, distributed correctness, tests, and documentation.
- [ ] Correct all verified findings and rerun the complete verification matrix.
- [ ] Add CI for non-Docker tests and Compose validation.
- [ ] Commit the verified course baseline.

## Plan Self-Review

- Every approved subsystem maps to a task and an executable test layer.
- Source evidence stays external and read-only.
- ML, firmware, and speculative research extensions are excluded.
- Identity, delivery, deduplication, retention, and batch reconciliation use
  separate contracts.
- All behavior tasks begin with a failing test and include a concrete validation
  command.
