# Reproducibility Guide

## Reproduction Boundary

This guide reproduces the telemetry data system. It does not retrain TinyML
models or rerun hardware inference. The input is a sealed evidence bundle from
four completed board-model sessions.

The canonical input has 225,204 physical executions over one shared sequence
of 56,301 predictive rows. Kafka replay can create additional deliveries for
fault-injection experiments, but it cannot create new predictive evidence.

## Required Software

- Git
- Python 3.11 or newer
- Java 17 or newer
- Docker Desktop with Docker Compose
- at least 12 GB free memory for the complete local Compose platform
- enough free storage for container images, Kafka, HDFS, checkpoints, and
  Parquet outputs

Commands below assume PowerShell at the repository root. Use a new run ID and
topic for every measured experiment.

## 1. Install the Python Environment

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev,kafka,spark,dashboard]"
```

## 2. Build and Verify the Evidence Bundle

Set the source repository only for the one-time import. The importer is
read-only and refuses outputs under the source root.

```powershell
$env:CUKD_XAI_ROOT = "C:\path\to\CuKD-XAI"
.\.venv\Scripts\tinyml-ids-telemetry evidence audit --source --evidence-root $env:CUKD_XAI_ROOT
.\.venv\Scripts\tinyml-ids-telemetry evidence bundle --evidence-root $env:CUKD_XAI_ROOT --output data/source-evidence
$env:TINYML_IDS_EVIDENCE_ROOT = (Resolve-Path data/source-evidence).Path
.\.venv\Scripts\tinyml-ids-telemetry evidence audit
.\.venv\Scripts\tinyml-ids-telemetry evidence snapshot --output artifacts/canonical-snapshot-v1
```

The expected frozen identities are in
[`provenance/evidence-v1.md`](provenance/evidence-v1.md). Do not continue if an
identity, lineage, row count, prediction, or fixed-point logit check fails.

## 3. Verify Code and the Fixture Pipeline

```powershell
.\.venv\Scripts\ruff check src dashboard jobs tests
.\.venv\Scripts\ruff format --check src dashboard jobs tests
.\.venv\Scripts\mypy src dashboard
.\.venv\Scripts\mypy --explicit-package-bases jobs\spark\run_pipeline.py
.\.venv\Scripts\python -m pytest -m "not integration and not full_evidence" -q
.\scripts\run-full-evidence.ps1
.\scripts\run-fixture-e2e.ps1
```

The fixture run checks Kafka replay, Spark state and Parquet outputs, HDFS,
Hadoop Streaming, and service health. Fixture counts are software verification
results, not course experiment measurements.

## 4. Start the Platform and Stage the Snapshot

```powershell
.\scripts\bootstrap.ps1
docker compose cp artifacts/canonical-snapshot-v1 app:/workspace/artifacts/
```

The full-evidence script verifies the three snapshot files and their frozen
hashes before transfer. The replay command independently validates the staged
manifest, event stream, lineages, hashes, and counts before sending a Kafka
record.

## 5. Run One Spark Experiment

This example is the normal full-evidence run. A fresh topic prevents records
from earlier experiments from entering the throughput measurement.

```powershell
$RunId = "course-normal-001"
$Topic = "ids.telemetry.$RunId"
$QueryVersion = "course-v1-$RunId"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --partitions 1 --replication-factor 1 --topic $Topic
docker compose exec -T spark spark-submit --master "local[2]" --conf spark.ui.enabled=false --conf spark.jars.ivy=/opt/spark/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 jobs/spark/run_pipeline.py --artifact-root /workspace/artifacts/course --snapshot /workspace/artifacts/canonical-snapshot-v1 --run-id $RunId --start-utc 2026-08-24T00:00:00Z --interval-ms 10 --rate-per-second 1000 --out-of-order-window 1 --watermark-delay-ms 1 --bootstrap-servers kafka:19092 --topic $Topic --query-version $QueryVersion
```

The driver starts all four streaming queries before replay. It validates the
topic contract, waits for deterministic replay completion, drains each query,
stops streaming, performs exact Gold finalization, and atomically publishes the
run manifest.

For duplicate and out-of-order experiments, use a new run ID and topic. Add
`--duplicate-every N` for deterministic duplicate injection. For a reordering
window `W`, the watermark delay must be greater than
`interval_ms * (W - 1)`. For example, `--out-of-order-window 4` with a 10 ms
interval requires at least `--watermark-delay-ms 31`.

The complete experiment matrix and trial policy are defined in
[`experiment-protocol.md`](experiment-protocol.md).

## 6. Run the Hadoop Oracle

Stage only the canonical events and snapshot manifest in a new HDFS input
directory:

```powershell
docker compose exec -T hadoop hdfs dfs -mkdir -p /tinyml-ids/input/$RunId
docker compose exec -T hadoop hdfs dfs -put /workspace/artifacts/canonical-snapshot-v1/events.jsonl /tinyml-ids/input/$RunId/events.jsonl
docker compose exec -T hadoop hdfs dfs -put /workspace/artifacts/canonical-snapshot-v1/snapshot_manifest.json /tinyml-ids/input/$RunId/snapshot_manifest.json
docker compose exec -T hadoop bash /workspace/scripts/run-hadoop-oracle.sh /tinyml-ids/input/$RunId /tinyml-ids/output/$RunId
docker compose exec -T hadoop mkdir -p /workspace/artifacts/course/oracle/$RunId
docker compose exec -T hadoop hdfs dfs -get /tinyml-ids/output/$RunId/metrics/part-00000 /workspace/artifacts/course/oracle/$RunId/hadoop_metrics.jsonl
docker compose exec -T hadoop hdfs dfs -get /tinyml-ids/output/$RunId/hadoop_oracle_manifest.json /workspace/artifacts/course/oracle/$RunId/hadoop_oracle_manifest.json
```

The Hadoop mapper and reducer use standard input and output and do not import
Spark transformations.

## 7. Reconcile and Publish the Course Report

```powershell
$Finalized = "/workspace/artifacts/course/finalized/replay_run_id=$RunId"
$PipelineExecution = "/workspace/artifacts/course/runs/$RunId/pipeline_execution.json"
$Oracle = "/workspace/artifacts/course/oracle/$RunId"
$Reconciliation = "/workspace/artifacts/course/reconciliation/$RunId.json"
$ReportDirectory = "/workspace/artifacts/course/reports/$RunId"
docker compose exec -T app mkdir -p /workspace/artifacts/course/reconciliation /workspace/artifacts/course/reports
docker compose exec -T app python3 jobs/hadoop/reconcile.py --spark-aggregates "$Finalized/spark_aggregates.json" --hadoop-metrics "$Oracle/hadoop_metrics.jsonl" --finalized-run-manifest "$Finalized/finalized_run_manifest.json" --hadoop-run-manifest "$Oracle/hadoop_oracle_manifest.json" --output $Reconciliation
docker compose exec -T app python3 -m tinyml_ids_telemetry.cli.main report --finalized-run $Finalized --reconciliation $Reconciliation --pipeline-execution $PipelineExecution --output $ReportDirectory
```

A course result is final only when the finalized manifest is valid, all expected
outputs have success markers, the aggregate hash matches, and reconciliation
reports `matched=true` with an empty difference list.

## 8. Copy Final Results to the Host

```powershell
New-Item -ItemType Directory -Force "output\$RunId" | Out-Null
docker compose cp "app:$Finalized" "output\$RunId\finalized"
docker compose cp "app:$PipelineExecution" "output\$RunId\pipeline-execution.json"
docker compose cp "app:$Reconciliation" "output\$RunId\reconciliation.json"
docker compose cp "app:$ReportDirectory/course_report.json" "output\$RunId\course-report.json"
docker compose cp "app:$ReportDirectory/course_report.md" "output\$RunId\course-report.md"
```

Generated results remain ignored by Git until reviewed and deliberately added
as a compact baseline document. Raw evidence, Parquet datasets, checkpoints,
and service volumes must not be committed.

## 9. Open the Dashboard

```powershell
$env:TINYML_IDS_FINALIZED_RUN = (Resolve-Path "output\$RunId\finalized").Path
$env:TINYML_IDS_COURSE_REPORT = (Resolve-Path "output\$RunId\course-report.json").Path
.\.venv\Scripts\streamlit run dashboard\app.py
```

The dashboard reads finalized aggregates and the provenance-bound course
report. It does not scan raw telemetry.

## 10. Stop Services

```powershell
docker compose down
```

Do not add `--volumes` unless the Kafka, HDFS, Spark Ivy, and artifact volumes
are intentionally being discarded after their outputs have been copied and
verified.
