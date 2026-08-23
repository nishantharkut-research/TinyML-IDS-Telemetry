"""Built-in Spark SQL retention-policy expressions."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from functools import reduce
from hashlib import sha256
from pathlib import Path
from typing import cast

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import DecimalType

from jobs.spark.common import IGNORED_COUNTER_COLUMNS
from tinyml_ids_telemetry.policies.security_aware import load_security_aware_policy

_PROBABILITY_TYPE = DecimalType(20, 10)
_POLICY_IDS = ("retain-all-v1", "security-aware-static-v1")


@dataclass(frozen=True, slots=True)
class PolicyConfigContract:
    """Identity of the exact versioned policy configuration used for derivation."""

    sha256: str
    policy_ids: tuple[str, ...]


def validate_policy_config(path: Path) -> PolicyConfigContract:
    """Validate the checked-in two-policy TOML contract without accepting extensions."""
    try:
        payload = path.read_bytes()
        data = cast(dict[str, object], tomllib.loads(payload.decode("utf-8")))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"invalid retention policy configuration: {error}") from error
    if set(data) != {"version", "retain_all", "security_aware_static"}:
        raise ValueError("retention policy configuration must contain exactly two policies")
    if data.get("version") != 1:
        raise ValueError("unsupported retention policy configuration version")
    if data.get("retain_all") != {"policy_id": "retain-all-v1"}:
        raise ValueError("retain_all policy contract mismatch")
    security = data.get("security_aware_static")
    expected_security_keys = {
        "policy_id",
        "normal_sample_numerator",
        "normal_sample_denominator",
        "threshold_snapshot_id",
        "threshold_quantile_method",
        "logit_frac_bits",
        "margin_thresholds",
    }
    if not isinstance(security, dict) or set(security) != expected_security_keys:
        raise ValueError("security_aware_static policy contract mismatch")
    load_security_aware_policy(path)
    return PolicyConfigContract(sha256(payload).hexdigest(), _POLICY_IDS)


def _protocol_anomaly() -> Column:
    ignored_total = reduce(
        lambda left, right: left + right,
        (F.col(name).cast("long") for name in IGNORED_COUNTER_COLUMNS),
    )
    return (F.col("attempts") > 1) | (F.col("response_timeout_count") > 0) | (ignored_total > 0)


def _normal_sample_selected() -> Column:
    digest = F.sha2(
        F.concat(F.lit("security-aware-static-v1|"), F.col("source_event_id")),
        256,
    )
    first_unsigned_u64 = F.conv(F.substring(digest, 1, 16), 16, 10).cast(DecimalType(20, 0))
    return F.pmod(first_unsigned_u64, F.lit(10)) < F.lit(1)


def _security_reason() -> Column:
    sorted_logits = F.sort_array(F.col("fixed_logits"), asc=False)
    margin_numerator = F.element_at(sorted_logits, 1) - F.element_at(sorted_logits, 2)
    threshold_numerator = F.when(F.col("student_model") == "student_A", F.lit(3241)).otherwise(
        F.lit(2919)
    )
    return (
        F.when(
            ~F.col("student_model").isin("student_A", "student_B"),
            F.raise_error("unsupported student_model for security-aware-static-v1"),
        )
        .when(
            F.col("logit_frac_bits") != 9,
            F.raise_error("logit_frac_bits mismatch for security-aware-static-v1"),
        )
        .when(F.col("predicted_class_id") != F.col("normal_class_id"), "predicted_attack")
        .when(F.col("status") != "OK", "device_status")
        .when(_protocol_anomaly(), "protocol_anomaly")
        .when(margin_numerator < threshold_numerator, "low_margin")
        .when(_normal_sample_selected(), "normal_sample")
        .otherwise("normal_not_sampled")
    )


def build_policy_decisions(silver: DataFrame) -> DataFrame:
    """Return two label-blind decision rows for every valid unique input row."""
    keys = silver.select(
        "replay_run_id",
        "source_event_id",
        "student_model",
        "status",
        "predicted_class_id",
        "normal_class_id",
        "fixed_logits",
        "logit_frac_bits",
        "attempts",
        "response_timeout_count",
        *IGNORED_COUNTER_COLUMNS,
    )
    retain_all = keys.select(
        "replay_run_id",
        "source_event_id",
        F.lit("retain-all-v1").alias("policy_id"),
        F.lit(True).alias("retained"),
        F.lit("retain_all").alias("reason"),
        F.lit(1).cast("int").alias("inclusion_probability_numerator"),
        F.lit(1).cast("int").alias("inclusion_probability_denominator"),
        F.lit("1.0").cast(_PROBABILITY_TYPE).alias("inclusion_probability"),
        F.lit("1.0").cast(_PROBABILITY_TYPE).alias("inverse_probability_weight"),
    )
    reason = _security_reason()
    mandatory = reason.isin(
        "predicted_attack",
        "device_status",
        "protocol_anomaly",
        "low_margin",
    )
    security = keys.select(
        "replay_run_id",
        "source_event_id",
        F.lit("security-aware-static-v1").alias("policy_id"),
        (reason != "normal_not_sampled").alias("retained"),
        reason.alias("reason"),
        F.lit(1).cast("int").alias("inclusion_probability_numerator"),
        F.when(mandatory, F.lit(1))
        .otherwise(F.lit(10))
        .cast("int")
        .alias("inclusion_probability_denominator"),
        F.when(mandatory, F.lit("1.0"))
        .otherwise(F.lit("0.1"))
        .cast(_PROBABILITY_TYPE)
        .alias("inclusion_probability"),
        F.when(mandatory, F.lit("1.0"))
        .otherwise(F.lit("10.0"))
        .cast(_PROBABILITY_TYPE)
        .alias("inverse_probability_weight"),
    )
    return retain_all.unionByName(security)
