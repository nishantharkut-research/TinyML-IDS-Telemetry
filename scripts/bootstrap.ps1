$ErrorActionPreference = "Stop"

$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $RepositoryRoot "compose.yaml"
$Verifier = Join-Path $RepositoryRoot "scripts\verify-services.py"
$Python = Join-Path $RepositoryRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    $Python = "python"
}

& docker compose -f $ComposeFile up --detach --build --wait kafka hadoop spark app
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }
& $Python $Verifier
if ($LASTEXITCODE -ne 0) { throw "service verification failed" }
& docker compose -f $ComposeFile exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --partitions 1 --replication-factor 1 --topic ids.telemetry.raw
if ($LASTEXITCODE -ne 0) { throw "Kafka topic bootstrap failed" }
$TopicDescription = (& docker compose -f $ComposeFile exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic ids.telemetry.raw | Out-String)
if ($LASTEXITCODE -ne 0) { throw "Kafka topic verification failed" }
if ($TopicDescription -notmatch 'PartitionCount:\s+1(?:\s|$)') {
    throw "Kafka topic ids.telemetry.raw must have exactly one partition"
}
if ($TopicDescription -notmatch 'ReplicationFactor:\s+1(?:\s|$)') {
    throw "Kafka topic ids.telemetry.raw must have replication factor one"
}
& docker compose -f $ComposeFile exec -T hadoop hdfs dfs -mkdir -p /tinyml-ids/input /tinyml-ids/output /tinyml-ids/tests
if ($LASTEXITCODE -ne 0) { throw "HDFS bootstrap failed" }
& $Python $Verifier
if ($LASTEXITCODE -ne 0) { throw "post-bootstrap service verification failed" }
