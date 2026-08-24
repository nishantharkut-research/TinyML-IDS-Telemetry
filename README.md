# TinyML IDS Telemetry

**Auditable stream and batch analytics for hardware-validated TinyML intrusion telemetry**

TinyML IDS Telemetry is an IoT and Big Data Management course project built on
sealed hardware telemetry from completed TinyML intrusion-detection runs. It
ingests deterministic event replay through Kafka, processes it with Spark
Structured Streaming, stores typed data in Parquet and HDFS, verifies final
aggregates with an independent Hadoop Streaming job, and presents only
finalized results through a compact dashboard.

## Scope

The evidence contains four board-model execution lineages. Each lineage has
56,301 physical executions over the same ordered predictive sequence.

| Quantity | Value | Meaning |
| --- | ---: | --- |
| Board-model lineages | 4 | Two student models on two MCU boards |
| Hardware executions | 225,204 | Four physical execution lineages |
| Unique predictive rows | 56,301 | Rows in the shared evaluation sequence |
| Kafka deliveries | Run dependent | Includes controlled duplicate deliveries |

The 225,204 executions are not 225,204 independent predictive samples. This
project begins after inference. It does not retrain a model, alter firmware,
capture live packets, or modify the source research repository.

## Data Flow

```text
Verified evidence bundle
        |
        v
Canonical telemetry snapshot --> deterministic Kafka replay
                                      |
                                      v
                            Bronze delivery archive
                                      |
                         +------------+------------+
                         |                         |
                         v                         v
                Spark Silver/Gold          Quarantine records
                         |
              Parquet, SQL, retention views
                         |
                         v
                Finalized report/dashboard

Canonical snapshot --> HDFS --> Hadoop Streaming oracle
                                      |
                                      v
                         Spark-Hadoop reconciliation
```

Bronze preserves Kafka deliveries. Silver validates and performs
watermark-bounded deduplication. Gold performs exact end-of-run deduplication
by stable source identity and publishes immutable aggregates. Hadoop recomputes
the exact count and integer-sum metrics without reusing Spark transformations.

## Implemented Baseline

- hash-verified import and canonical snapshot generation
- deterministic replay with duplicate and out-of-order schedules
- one-partition Kafka correctness contract and completion receipts
- Spark Bronze, Silver, quarantine, policy, and exact Gold publication
- HDFS and Hadoop Streaming batch oracle
- field-level Spark-Hadoop reconciliation
- retain-all and security-aware retention policies
- immutable execution measurements for phase time, achieved rate, and output bytes
- provenance-bound report CLI and read-only Streamlit dashboard
- pinned Docker Compose platform with persistent named volumes
- unit, property, contract, and distributed fixture verification

The full measured course experiment matrix is intentionally deferred to the
designated execution machine. No throughput or storage result is claimed before
that run is finalized and reconciled. The current verified status is recorded
in [`docs/results/course-baseline.md`](docs/results/course-baseline.md).

## Local Setup

Requirements are Python 3.11 or newer, Java 17 or newer, Docker Desktop, and
Docker Compose.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev,kafka,spark,dashboard]"
.\scripts\bootstrap.ps1
```

Create the read-only evidence bundle and canonical snapshot once:

```powershell
$env:CUKD_XAI_ROOT = "C:\path\to\CuKD-XAI"
.\.venv\Scripts\tinyml-ids-telemetry evidence audit --source --evidence-root $env:CUKD_XAI_ROOT
.\.venv\Scripts\tinyml-ids-telemetry evidence bundle --evidence-root $env:CUKD_XAI_ROOT --output data/source-evidence
$env:TINYML_IDS_EVIDENCE_ROOT = (Resolve-Path data/source-evidence).Path
.\.venv\Scripts\tinyml-ids-telemetry evidence audit
.\.venv\Scripts\tinyml-ids-telemetry evidence snapshot --output artifacts/canonical-snapshot-v1
```

The importer preserves 79 required artifacts with repository-relative paths
and SHA-256 values. Generated data and source evidence are excluded from Git.

Run the local fixture verification:

```powershell
.\scripts\run-fixture-e2e.ps1
```

The complete production sequence is documented in
[`docs/reproducibility.md`](docs/reproducibility.md). Experiment definitions and
measurement units are fixed in
[`docs/experiment-protocol.md`](docs/experiment-protocol.md).

## Development Checks

```powershell
.\.venv\Scripts\ruff check src dashboard jobs tests
.\.venv\Scripts\ruff format --check src dashboard jobs tests
.\.venv\Scripts\mypy src dashboard
.\.venv\Scripts\mypy --explicit-package-bases jobs\spark\run_pipeline.py
.\.venv\Scripts\python -m pytest -m "not integration and not full_evidence" -q
docker compose -f compose.yaml config --quiet
```

## Documentation

- [System design](docs/design/2026-08-22-course-system.md)
- [Evidence provenance](docs/provenance/evidence-v1.md)
- [Course experiment protocol](docs/experiment-protocol.md)
- [IoT and BDM concept map](docs/course-concepts.md)
- [Reproducibility guide](docs/reproducibility.md)
- [Verified baseline status](docs/results/course-baseline.md)
- [Post-course research boundary](docs/research-extension.md)
- [One-page course brief](docs/coursework/iotbdm-project-brief.pdf)

## Claim Boundary

Pipeline throughput is not MCU throughput. Replay timestamps are synthetic
event time, not packet-capture time. Security-aware retention reduces
gateway-to-backend storage, not completed MCU-to-gateway traffic. True-label
retention is reported only as offline evaluation.

## License

Copyright (c) 2026 Nishant Harkut. All rights reserved. No open-source license
is granted at this stage.
