#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/.." && pwd)
python_bin="$repository_root/.venv/bin/python"

if [[ ! -x "$python_bin" ]]; then
    printf 'Create the repository .venv before running full evidence\n' >&2
    exit 69
fi
if [[ -z "${TINYML_IDS_EVIDENCE_ROOT:-}" || ! -d "$TINYML_IDS_EVIDENCE_ROOT" ]]; then
    printf 'TINYML_IDS_EVIDENCE_ROOT must identify the verified evidence source or bundle\n' >&2
    exit 64
fi
TINYML_IDS_EVIDENCE_ROOT=$(cd -- "$TINYML_IDS_EVIDENCE_ROOT" && pwd -P)
export TINYML_IDS_EVIDENCE_ROOT

"$repository_root/scripts/bootstrap.sh"
cd -- "$repository_root"
"$python_bin" -m pytest tests/full_evidence/test_cukd_xai.py -q

snapshot="$repository_root/artifacts/canonical-snapshot-v1"
if [[ ! -d "$snapshot" ]]; then
    "$python_bin" -m tinyml_ids_telemetry.cli.main evidence snapshot \
        --evidence-root "$TINYML_IDS_EVIDENCE_ROOT" \
        --output "$snapshot"
fi

expected_snapshot_id=d4cfb79165bcc88e82f3aae0721d86b9cd5fd008f5567d11d5e6bc98e6dfb4ff
expected_events_hash=dd790ff824e2c53e9661da9e03a6f0d05eb78fc61b36e10c57ac4b6d83314f8f
expected_lineages_hash=92fe3ae37fadf616e7e9d7e3e043feec792a1d355cf044c87aac03245136daab
expected_files=$'events.jsonl\nlineages.jsonl\nsnapshot_manifest.json'
observed_files=$(find "$snapshot" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | sort)
if [[ "$observed_files" != "$expected_files" ]] || \
    find "$snapshot" -mindepth 1 -maxdepth 1 -type d -print -quit | grep -q .; then
    printf 'canonical snapshot must contain exactly the three contract files\n' >&2
    exit 65
fi

events_hash=$(sha256sum "$snapshot/events.jsonl" | awk '{print $1}')
lineages_hash=$(sha256sum "$snapshot/lineages.jsonl" | awk '{print $1}')
event_count=$(wc -l < "$snapshot/events.jsonl")
lineage_count=$(wc -l < "$snapshot/lineages.jsonl")
if [[ "$events_hash" != "$expected_events_hash" || \
      "$lineages_hash" != "$expected_lineages_hash" || \
      "$event_count" -ne 225204 || "$lineage_count" -ne 4 ]]; then
    printf 'canonical snapshot files do not match the frozen full-evidence contract\n' >&2
    exit 65
fi

"$python_bin" - "$snapshot/snapshot_manifest.json" \
    "$expected_snapshot_id" "$expected_events_hash" "$expected_lineages_hash" <<'PY'
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
expected = {
    "schema_version": "telemetry-snapshot-v1",
    "snapshot_id": sys.argv[2],
    "events_sha256": sys.argv[3],
    "lineages_sha256": sys.argv[4],
    "event_count": 225_204,
    "lineage_count": 4,
}
if any(manifest.get(key) != value for key, value in expected.items()):
    raise SystemExit("canonical snapshot manifest does not match the frozen contract")
PY
