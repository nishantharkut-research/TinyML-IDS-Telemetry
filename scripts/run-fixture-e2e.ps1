$ErrorActionPreference = "Stop"

$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $RepositoryRoot "compose.yaml"
$Verifier = Join-Path $RepositoryRoot "scripts\verify-services.py"
$Bootstrap = Join-Path $RepositoryRoot "scripts\bootstrap.ps1"
$Python = Join-Path $RepositoryRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    $Python = "python"
}

& $Bootstrap
& docker compose -f $ComposeFile exec -T -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 -e TINYML_IDS_REQUIRE_KAFKA_INTEGRATION=1 app python3 -m pytest tests/integration/test_kafka_replay.py -q
if ($LASTEXITCODE -ne 0) { throw "Kafka fixture integration failed" }
& docker compose -f $ComposeFile exec -T -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 -e KAFKA_TOPIC=ids.telemetry.fixture -e TINYML_IDS_REQUIRE_SPARK_INTEGRATION=1 spark python3 -m pytest tests/integration/test_spark_pipeline.py -q
if ($LASTEXITCODE -ne 0) { throw "Spark fixture integration failed" }
& docker compose -f $ComposeFile exec -T -e HADOOP_HOME=/opt/hadoop -e TINYML_IDS_HDFS_TEST_ROOT=/tinyml-ids/tests -e TINYML_IDS_RUN_HADOOP_INTEGRATION=1 hadoop python3 -m pytest tests/integration/test_hadoop_streaming.py -q
if ($LASTEXITCODE -ne 0) { throw "Hadoop fixture integration failed" }
& $Python $Verifier
if ($LASTEXITCODE -ne 0) { throw "service verification failed after fixture run" }
