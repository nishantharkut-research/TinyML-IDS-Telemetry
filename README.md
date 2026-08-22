# TinyML IDS Telemetry

**Real-Time and Batch Big-Data Analytics for Hardware-Validated TinyML IoT
Intrusion Telemetry**

This repository builds a reproducible data-engineering system over completed
TinyML hardware telemetry. It connects deterministic evidence replay, Apache
Kafka, Spark Structured Streaming, HDFS and Parquet storage, Hadoop Streaming
verification, versioned retention policies, SQL analytics, and a compact
dashboard.

## Project Boundary

The source evidence comes from four completed CuKD-XAI Wi-Fi hardware sessions:
two fixed-point student models executed on an ESP32-C3 and an Arduino R4 WiFi.
Each board-model pair contains 56,301 full-stage executions. The resulting
225,204 records are repeated physical executions over the same test set, not
225,204 independent predictive samples.

This project begins after inference. It does not retrain models, modify firmware,
capture live packets, extract network features, or alter the archived evidence.

## Architecture

```text
CuKD-XAI evidence -> canonical snapshot -> Kafka replay
                                        -> Bronze deliveries
                                        -> Spark Silver/Gold
                                        -> retained views
                                        -> SQL/dashboard

canonical snapshot -> Hadoop Streaming oracle -> reconciliation
```

The course baseline demonstrates at-least-once delivery, deterministic event
identity, stateful deduplication, event-time windows, watermarks, Parquet/HDFS,
MapReduce verification, and storage-versus-query trade-offs.

## Status

The repository is under active implementation on the course-system foundation.
The approved system boundary and acceptance criteria are documented in
[`docs/design/2026-08-22-course-system.md`](docs/design/2026-08-22-course-system.md).

## Development

Use Python 3.11 or newer:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev]"
.\.venv\Scripts\python -m pytest
```

Full-evidence runs require `CUKD_XAI_ROOT` to point to a read-only CuKD-XAI
checkout. Distributed integration tests additionally require Docker Desktop.

## Evidence and Claims

All reports must distinguish source evidence from analytics replay, physical
executions from independent samples, MCU compute time from Wi-Fi round-trip
time, and predicted-alert preservation from true-attack retention. See
[`AGENTS.md`](AGENTS.md) for non-negotiable engineering and reporting rules.

## License

Copyright (c) 2026 Nishant Harkut. All rights reserved. No open-source license
is granted by this repository at this stage.

