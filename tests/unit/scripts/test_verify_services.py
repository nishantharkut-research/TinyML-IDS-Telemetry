from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

PROJECT_ROOT = Path(__file__).parents[3]
MODULE_PATH = PROJECT_ROOT / "scripts" / "verify-services.py"


def _module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_services", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError("could not load service verifier")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rows(*, hadoop_health: str = "healthy") -> list[dict[str, str]]:
    return [
        {"Service": "app", "State": "running", "Health": "healthy"},
        {"Service": "hadoop", "State": "running", "Health": hadoop_health},
        {"Service": "kafka", "State": "running", "Health": "healthy"},
        {"Service": "spark", "State": "running", "Health": "healthy"},
    ]


def test_parse_compose_ps_accepts_array_and_json_lines() -> None:
    module = _module()
    rows = _rows()

    assert module.parse_compose_ps(json.dumps(rows)) == rows
    assert module.parse_compose_ps("\n".join(json.dumps(row) for row in rows)) == rows


def test_validate_services_requires_exact_running_healthy_membership() -> None:
    module = _module()

    assert module.validate_services(_rows()) == {
        "app": {"health": "healthy", "state": "running"},
        "hadoop": {"health": "healthy", "state": "running"},
        "kafka": {"health": "healthy", "state": "running"},
        "spark": {"health": "healthy", "state": "running"},
    }

    with pytest.raises(module.ServiceVerificationError, match="missing services: spark"):
        module.validate_services(_rows()[:-1])
    with pytest.raises(module.ServiceVerificationError, match="unexpected services: rogue"):
        module.validate_services(
            [*_rows(), {"Service": "rogue", "State": "running", "Health": "healthy"}]
        )
    with pytest.raises(module.ServiceVerificationError, match="hadoop is running/unhealthy"):
        module.validate_services(_rows(hadoop_health="unhealthy"))


def test_wait_for_services_retries_then_returns_validated_state() -> None:
    module = _module()
    attempts = iter(
        [
            module.ServiceVerificationError("compose unavailable"),
            json.dumps(_rows(hadoop_health="starting")),
            json.dumps(_rows()),
        ]
    )
    sleeps: list[float] = []

    def inspect() -> str:
        value = next(attempts)
        if isinstance(value, Exception):
            raise value
        return value

    result = module.wait_for_services(
        timeout_seconds=5,
        poll_seconds=0.25,
        inspect=inspect,
        monotonic=iter([0.0, 0.1, 0.2, 0.3]).__next__,
        sleep=sleeps.append,
    )

    assert result["hadoop"] == {"health": "healthy", "state": "running"}
    assert sleeps == [0.25, 0.25]


def test_wait_for_services_retries_compose_inspection_timeouts() -> None:
    module = _module()
    attempts = iter(
        [
            subprocess.TimeoutExpired(["docker", "compose", "ps"], timeout=30),
            json.dumps(_rows()),
        ]
    )
    sleeps: list[float] = []

    def inspect() -> str:
        value = next(attempts)
        if isinstance(value, Exception):
            raise value
        return value

    result = module.wait_for_services(
        timeout_seconds=5,
        poll_seconds=0.25,
        inspect=inspect,
        monotonic=iter([0.0, 0.1, 0.2]).__next__,
        sleep=sleeps.append,
    )

    assert result["spark"] == {"health": "healthy", "state": "running"}
    assert sleeps == [0.25]


def test_wait_for_services_reports_the_last_failure_at_timeout() -> None:
    module = _module()

    with pytest.raises(
        module.ServiceVerificationError,
        match=(
            r"services did not become healthy within 1\.0 seconds: "
            r"hadoop is running/unhealthy"
        ),
    ):
        module.wait_for_services(
            timeout_seconds=1.0,
            poll_seconds=0.25,
            inspect=lambda: json.dumps(_rows(hadoop_health="unhealthy")),
            monotonic=iter([0.0, 0.5, 1.1]).__next__,
            sleep=lambda _: None,
        )


def test_wait_for_services_rejects_success_observed_after_deadline() -> None:
    module = _module()

    with pytest.raises(
        module.ServiceVerificationError,
        match=r"services did not become healthy within 1\.0 seconds",
    ):
        module.wait_for_services(
            timeout_seconds=1.0,
            poll_seconds=0.25,
            inspect=lambda: json.dumps(_rows()),
            monotonic=iter([0.0, 1.1]).__next__,
            sleep=lambda _: None,
        )
