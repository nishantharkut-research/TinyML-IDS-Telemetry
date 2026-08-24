from __future__ import annotations

import json
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
COMPOSE_FILE = PROJECT_ROOT / "compose.yaml"
SPARK_DOCKERFILE = PROJECT_ROOT / "infra" / "spark" / "Dockerfile"
HADOOP_DOCKERFILE = PROJECT_ROOT / "infra" / "hadoop" / "Dockerfile"
HADOOP_ENTRYPOINT = PROJECT_ROOT / "infra" / "hadoop" / "hadoop-entrypoint.sh"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"
HADOOP_CONF = PROJECT_ROOT / "infra" / "hadoop" / "conf"
FULL_EVIDENCE_SHELL = PROJECT_ROOT / "scripts" / "run-full-evidence.sh"
CONTAINER_CONSTRAINTS = PROJECT_ROOT / "infra" / "container-constraints.txt"

KAFKA_IMAGE = (
    "apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e"
)
SPARK_BASE = (
    "apache/spark:4.1.3-python3@"
    "sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211"
)
PYTHON_VERSION = "3.11.16"
PYTHON_SHA256 = "91bcdebfdde239a003ae93738a7fce0f9230fee5c4bc2b86f6e6e8c6f98aabe8"
HADOOP_BASE = (
    "eclipse-temurin:17-jre-noble@"
    "sha256:5726444f65e67c407bac4ff1e036b475051f50bb3840273e81814e77c6fa8ee3"
)
HADOOP_SHA512 = (
    "04ab94496cc00c8b7a28d03f6308eff8d2a4e7f37a9da5e8e086e4d6fc990e7a"
    "94d661908f6a6136039536efb362614b8aecdef185b5fb8ed588f0b152c7aa16"
)
EXPECTED_EVENTS_SHA256 = "dd790ff824e2c53e9661da9e03a6f0d05eb78fc61b36e10c57ac4b6d83314f8f"
EXPECTED_LINEAGES_SHA256 = "92fe3ae37fadf616e7e9d7e3e043feec792a1d355cf044c87aac03245136daab"


def _compose_config() -> dict[str, Any]:
    docker = shutil.which("docker")
    if docker is None:
        pytest.fail("Docker Compose CLI is required for the Compose contract test")
    completed = subprocess.run(
        [docker, "compose", "-f", str(COMPOSE_FILE), "config", "--format", "json"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        pytest.fail(f"docker compose config failed: {completed.stderr.strip()}")
    value = json.loads(completed.stdout)
    assert isinstance(value, dict)
    return value


def _volume_targets(service: dict[str, Any]) -> dict[str, str]:
    return {
        str(volume["target"]): str(volume["source"])
        for volume in service.get("volumes", [])
        if volume.get("type") == "volume"
    }


def _tmpfs_targets(service: dict[str, Any]) -> set[str]:
    return {
        str(volume["target"])
        for volume in service.get("volumes", [])
        if volume.get("type") == "tmpfs"
    }


def _xml_properties(path: Path) -> dict[str, str]:
    root = ET.parse(path).getroot()
    return {
        str(property_node.findtext("name")): str(property_node.findtext("value"))
        for property_node in root.findall("property")
    }


def test_compose_declares_the_exact_pinned_local_platform() -> None:
    config = _compose_config()
    services = config["services"]

    compose_source = COMPOSE_FILE.read_text(encoding="utf-8")
    assert re.search(r"(?m)^name:", compose_source) is None
    assert set(services) == {"app", "hadoop", "kafka", "spark"}
    assert services["kafka"]["image"] == KAFKA_IMAGE
    assert services["hadoop"]["image"] == "tinyml-ids-telemetry-hadoop:3.5.0"
    assert services["spark"]["image"] == "tinyml-ids-telemetry-spark:4.1.3"
    assert services["app"]["image"] == "tinyml-ids-telemetry-spark:4.1.3"

    expected_pythonpath = (
        "/opt/spark/python/lib/pyspark.zip:"
        "/opt/spark/python/lib/py4j-0.10.9.9-src.zip:"
        "/workspace/src:/workspace"
    )
    assert services["spark"]["environment"]["PYTHONPATH"] == expected_pythonpath
    assert services["app"]["environment"]["PYTHONPATH"] == expected_pythonpath

    assert services["hadoop"]["build"]["dockerfile"] == "infra/hadoop/Dockerfile"
    assert services["spark"]["build"]["dockerfile"] == "infra/spark/Dockerfile"
    assert services["app"]["build"]["dockerfile"] == "infra/spark/Dockerfile"


def test_service_dependencies_health_checks_and_loopback_ports_are_explicit() -> None:
    services = _compose_config()["services"]

    for service_name in ("kafka", "hadoop", "spark", "app"):
        healthcheck = services[service_name].get("healthcheck")
        assert isinstance(healthcheck, dict), service_name
        assert healthcheck.get("test"), service_name
        assert services[service_name]["restart"] == "unless-stopped"

    assert services["app"]["depends_on"] == {
        "hadoop": {"condition": "service_healthy", "required": True},
        "kafka": {"condition": "service_healthy", "required": True},
        "spark": {"condition": "service_healthy", "required": True},
    }
    assert services["spark"]["depends_on"] == {
        "kafka": {"condition": "service_healthy", "required": True}
    }
    spark_health = services["spark"]["healthcheck"]["test"]
    assert spark_health[0] == "CMD-SHELL"
    assert "java -version" in spark_health[1]
    assert "spark-submit" in spark_health[1]
    assert "org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.3.jar" in spark_health[1]
    assert "pyspark.__version__ == '4.1.3'" in spark_health[1]

    published = [port for service in services.values() for port in service.get("ports", [])]
    assert published
    assert all(port["host_ip"] == "127.0.0.1" for port in published)

    expected_limits = {
        "app": {"cpus": 0.5, "mem_limit": 512 * 1024 * 1024},
        "hadoop": {"cpus": 2.0, "mem_limit": 4 * 1024 * 1024 * 1024},
        "kafka": {"cpus": 1.0, "mem_limit": 1536 * 1024 * 1024},
        "spark": {"cpus": 2.0, "mem_limit": 3 * 1024 * 1024 * 1024},
    }
    for service_name, limits in expected_limits.items():
        assert services[service_name]["cpus"] == limits["cpus"]
        assert int(services[service_name]["mem_limit"]) == limits["mem_limit"]


def test_state_uses_only_declared_named_volumes() -> None:
    config = _compose_config()
    services = config["services"]
    declared = set(config["volumes"])

    assert declared == {
        "hadoop_datanode",
        "hadoop_namenode",
        "kafka_data",
        "spark_ivy",
        "telemetry_artifacts",
    }
    assert _volume_targets(services["kafka"]) == {"/var/lib/kafka/data": "kafka_data"}
    assert _volume_targets(services["hadoop"]) == {
        "/opt/hadoop/data/datanode": "hadoop_datanode",
        "/opt/hadoop/data/namenode": "hadoop_namenode",
        "/workspace/artifacts": "telemetry_artifacts",
    }
    assert _volume_targets(services["spark"]) == {
        "/home/spark/.ivy2": "spark_ivy",
        "/workspace/artifacts": "telemetry_artifacts",
    }
    assert _volume_targets(services["app"]) == {
        "/home/spark/.ivy2": "spark_ivy",
        "/workspace/artifacts": "telemetry_artifacts",
    }
    assert _tmpfs_targets(services["kafka"]) == {
        "/etc/kafka/secrets",
        "/mnt/shared/config",
    }
    assert services["kafka"].get("tmpfs") is None
    assert all(
        volume.get("type") != "bind"
        for service in services.values()
        for volume in service.get("volumes", [])
    )


def test_compose_contains_no_credentials_or_floating_remote_images() -> None:
    config = _compose_config()

    assert "secrets" not in config
    credential_pattern = re.compile(r"(?i)(password|passwd|secret|token)")
    for service_name, service in config["services"].items():
        environment = service.get("environment", {})
        assert isinstance(environment, dict)
        assert all(credential_pattern.search(str(key)) is None for key in environment)
        assert all(credential_pattern.search(str(value)) is None for value in environment.values())
        image = str(service["image"])
        assert ":latest" not in image
        if service_name == "kafka":
            assert "@sha256:" in image
        else:
            assert image.startswith("tinyml-ids-telemetry-")


def test_dockerfiles_pin_upstream_artifacts_and_exclude_source_evidence() -> None:
    spark = SPARK_DOCKERFILE.read_text(encoding="utf-8")
    hadoop = HADOOP_DOCKERFILE.read_text(encoding="utf-8")
    ignored = {
        line.strip().rstrip("/")
        for line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert f"FROM {SPARK_BASE}" in spark
    assert f"FROM {HADOOP_BASE}" in hadoop
    assert "hadoop-3.5.0.tar.gz" in hadoop
    assert HADOOP_SHA512 in hadoop
    assert "chown -R hadoop:hadoop /opt/hadoop /workspace" not in hadoop
    assert (
        "chown -R hadoop:hadoop /opt/hadoop/data /opt/hadoop/logs "
        "/opt/hadoop/pids /workspace /home/hadoop"
    ) in hadoop
    assert {".git", ".venv", "artifacts", "data"} <= ignored


def test_container_python_dependencies_are_exactly_constrained() -> None:
    constraints = {
        line.strip()
        for line in CONTAINER_CONSTRAINTS.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }
    assert "jsonschema==4.26.0" in constraints
    assert "kafka-python==3.0.11" in constraints
    assert "pytest==9.1.1" in constraints
    assert all("==" in requirement for requirement in constraints)

    for dockerfile in (SPARK_DOCKERFILE, HADOOP_DOCKERFILE):
        source = dockerfile.read_text(encoding="utf-8")
        assert "COPY infra/container-constraints.txt ./infra/container-constraints.txt" in source
        assert "--constraint infra/container-constraints.txt" in source


def test_spark_image_installs_a_supported_python_runtime() -> None:
    spark = SPARK_DOCKERFILE.read_text(encoding="utf-8")

    assert f"ARG PYTHON_VERSION={PYTHON_VERSION}" in spark
    assert f"ARG PYTHON_SHA256={PYTHON_SHA256}" in spark
    assert "www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz" in spark
    assert 'echo "${PYTHON_SHA256}  /tmp/python.tar.xz" | sha256sum -c -' in spark
    assert "COPY --from=python-builder /usr/local /usr/local" in spark
    assert "ENV PATH=/opt/spark/bin:/opt/spark/sbin:/usr/local/bin:${PATH}" in spark
    assert "/opt/spark/python/lib/pyspark.zip" in spark
    assert "/opt/spark/python/lib/py4j-0.10.9.9-src.zip" in spark
    assert "import ctypes, ssl, sys" in spark
    assert "sys.version_info[:3] == (3, 11, 16)" in spark
    assert "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3" in spark
    assert "spark.jars.ivy=/opt/spark/ivy" in spark
    assert "spark.sql.shuffle.partitions 4" in spark
    assert "mkdir -p /opt/spark/conf /opt/spark/ivy" in spark


def test_single_node_yarn_memory_contract_leaves_capacity_for_tasks() -> None:
    yarn = _xml_properties(HADOOP_CONF / "yarn-site.xml")
    mapred = _xml_properties(HADOOP_CONF / "mapred-site.xml")

    assert yarn["yarn.nodemanager.resource.memory-mb"] == "1536"
    assert yarn["yarn.scheduler.maximum-allocation-mb"] == "1536"
    assert yarn["yarn.scheduler.minimum-allocation-mb"] == "256"
    assert mapred["yarn.app.mapreduce.am.resource.mb"] == "384"
    assert mapred["mapreduce.map.memory.mb"] == "384"
    assert mapred["mapreduce.reduce.memory.mb"] == "384"
    assert mapred["yarn.app.mapreduce.am.command-opts"] == "-Xmx256m"
    assert mapred["mapreduce.map.java.opts"] == "-Xmx256m"
    assert mapred["mapreduce.reduce.java.opts"] == "-Xmx256m"
    assert "mapreduce.job.am.resource.mb" not in mapred
    assert "mapreduce.job.am.command-opts" not in mapred


def test_hadoop_exports_the_mapreduce_home_to_yarn_containers() -> None:
    dockerfile = HADOOP_DOCKERFILE.read_text(encoding="utf-8")
    yarn = _xml_properties(HADOOP_CONF / "yarn-site.xml")
    mapred = _xml_properties(HADOOP_CONF / "mapred-site.xml")

    assert "HADOOP_MAPRED_HOME=/opt/hadoop" in dockerfile
    assert "HADOOP_MAPRED_HOME" in yarn["yarn.nodemanager.env-whitelist"].split(",")
    assert (
        "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*" in mapred["mapreduce.application.classpath"]
    )


def test_hadoop_entrypoint_waits_for_namenode_rpc_before_safe_mode() -> None:
    entrypoint = HADOOP_ENTRYPOINT.read_text(encoding="utf-8")

    assert "wait_for_namenode" in entrypoint
    assert "hdfs dfsadmin -safemode get" in entrypoint
    assert "NameNode RPC did not become ready" in entrypoint
    assert entrypoint.index("wait_for_namenode") < entrypoint.index("hdfs dfsadmin -safemode wait")
    assert "trap stop_services EXIT" in entrypoint
    assert "trap 'exit 0' INT TERM" in entrypoint


def test_bootstrap_and_run_scripts_are_cross_platform_and_fail_fast() -> None:
    script_pairs = (
        (PROJECT_ROOT / "scripts" / "bootstrap.sh", PROJECT_ROOT / "scripts" / "bootstrap.ps1"),
        (
            PROJECT_ROOT / "scripts" / "run-fixture-e2e.sh",
            PROJECT_ROOT / "scripts" / "run-fixture-e2e.ps1",
        ),
    )
    for shell_script, powershell_script in script_pairs:
        shell = shell_script.read_text(encoding="utf-8")
        powershell = powershell_script.read_text(encoding="utf-8")
        assert "set -euo pipefail" in shell
        assert '$ErrorActionPreference = "Stop"' in powershell
        assert "verify-services.py" in shell
        assert "verify-services.py" in powershell

    full_evidence = (PROJECT_ROOT / "scripts" / "run-full-evidence.ps1").read_text(encoding="utf-8")
    assert '$ErrorActionPreference = "Stop"' in full_evidence
    assert "TINYML_IDS_EVIDENCE_ROOT" in full_evidence
    assert "d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff" in full_evidence
    assert EXPECTED_EVENTS_SHA256 in full_evidence
    assert EXPECTED_LINEAGES_SHA256 in full_evidence
    assert "Get-FileHash" in full_evidence
    assert "ReadLines" in full_evidence
    assert "Assert-CanonicalSnapshot" in full_evidence
    assert "Resolve-Path -LiteralPath $SourceRoot" in full_evidence
    assert "$env:TINYML_IDS_EVIDENCE_ROOT = $SourceRoot" in full_evidence

    full_evidence_shell = FULL_EVIDENCE_SHELL.read_text(encoding="utf-8")
    assert "set -euo pipefail" in full_evidence_shell
    assert "TINYML_IDS_EVIDENCE_ROOT" in full_evidence_shell
    assert "d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff" in full_evidence_shell
    assert EXPECTED_EVENTS_SHA256 in full_evidence_shell
    assert EXPECTED_LINEAGES_SHA256 in full_evidence_shell

    bootstrap = (PROJECT_ROOT / "scripts" / "bootstrap.sh").read_text(encoding="utf-8")
    assert "--if-not-exists" in bootstrap
    assert "--describe" in bootstrap
    assert "PartitionCount:" in bootstrap
    assert "ReplicationFactor:" in bootstrap
    assert "dfs -mkdir -p" in bootstrap
    powershell_bootstrap = (PROJECT_ROOT / "scripts" / "bootstrap.ps1").read_text(encoding="utf-8")
    assert "--describe" in powershell_bootstrap
    assert "PartitionCount:" in powershell_bootstrap
    assert "ReplicationFactor:" in powershell_bootstrap

    for fixture_runner in (
        PROJECT_ROOT / "scripts" / "run-fixture-e2e.sh",
        PROJECT_ROOT / "scripts" / "run-fixture-e2e.ps1",
    ):
        source = fixture_runner.read_text(encoding="utf-8")
        assert "TINYML_IDS_REQUIRE_KAFKA_INTEGRATION=1" in source
        assert "TINYML_IDS_REQUIRE_SPARK_INTEGRATION=1" in source
