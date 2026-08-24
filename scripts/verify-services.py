"""Wait for the exact local Compose platform to become running and healthy."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

EXPECTED_SERVICES = frozenset({"app", "hadoop", "kafka", "spark"})


class ServiceVerificationError(RuntimeError):
    """The local platform is absent, incomplete, or unhealthy."""


def parse_compose_ps(payload: str) -> list[dict[str, object]]:
    """Parse either Compose's JSON array or one-object-per-line output."""
    raw = payload.strip()
    if not raw:
        raise ServiceVerificationError("docker compose ps returned no services")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        try:
            value = [json.loads(line) for line in raw.splitlines() if line.strip()]
        except json.JSONDecodeError as error:
            raise ServiceVerificationError(
                f"docker compose ps returned invalid JSON: {error}"
            ) from error
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ServiceVerificationError("docker compose ps JSON must contain service objects")
    return cast(list[dict[str, object]], value)


def validate_services(rows: list[dict[str, object]]) -> dict[str, dict[str, str]]:
    """Require exactly the expected running and healthy services."""
    observed: dict[str, dict[str, str]] = {}
    for row in rows:
        service = row.get("Service")
        state = row.get("State")
        health = row.get("Health")
        if not all(isinstance(value, str) for value in (service, state, health)):
            raise ServiceVerificationError("compose service status fields must be strings")
        observed[cast(str, service)] = {
            "health": cast(str, health).casefold(),
            "state": cast(str, state).casefold(),
        }
    missing = sorted(EXPECTED_SERVICES - observed.keys())
    extra = sorted(observed.keys() - EXPECTED_SERVICES)
    if missing:
        raise ServiceVerificationError(f"missing services: {', '.join(missing)}")
    if extra:
        raise ServiceVerificationError(f"unexpected services: {', '.join(extra)}")
    for service in sorted(EXPECTED_SERVICES):
        status = observed[service]
        if status != {"health": "healthy", "state": "running"}:
            raise ServiceVerificationError(
                f"{service} is {status['state']}/{status['health']}, expected running/healthy"
            )
    return observed


def _inspect_compose() -> str:
    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        ["docker", "compose", "-f", str(root / "compose.yaml"), "ps", "--format", "json"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or "docker compose ps failed"
        raise ServiceVerificationError(message)
    return completed.stdout


def wait_for_services(
    *,
    timeout_seconds: float,
    poll_seconds: float,
    inspect: Callable[[], str] = _inspect_compose,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, dict[str, str]]:
    """Poll until the exact platform validates or the deadline expires."""
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ServiceVerificationError("timeouts must be positive")
    deadline = monotonic() + timeout_seconds
    last_error = ServiceVerificationError("services have not been inspected")
    while True:
        try:
            result = validate_services(parse_compose_ps(inspect()))
        except (OSError, subprocess.SubprocessError, ServiceVerificationError) as error:
            last_error = ServiceVerificationError(str(error))
            observed_at = monotonic()
            if observed_at < deadline:
                sleep(poll_seconds)
                continue
        else:
            observed_at = monotonic()
            if observed_at < deadline:
                return result
            last_error = ServiceVerificationError(
                "healthy status was observed only after the verification deadline"
            )
        message = (
            f"services did not become healthy within {timeout_seconds:.1f} seconds: {last_error}"
        )
        raise ServiceVerificationError(message) from last_error


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    arguments = parser.parse_args(argv)
    try:
        services = wait_for_services(
            timeout_seconds=arguments.timeout_seconds,
            poll_seconds=arguments.poll_seconds,
        )
    except ServiceVerificationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"services": services, "status": "healthy"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
