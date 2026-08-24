#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 || -z "${1:-}" || -z "${2:-}" ]]; then
    printf 'usage: %s HDFS_INPUT_DIRECTORY HDFS_OUTPUT_DIRECTORY\n' "$0" >&2
    exit 64
fi

hdfs_input=${1%/}
hdfs_output=${2%/}
if [[ "$hdfs_input" != /* || "$hdfs_output" != /* || "$hdfs_output" == / ]]; then
    printf 'HDFS input and output must be explicit absolute paths; output cannot be /\n' >&2
    exit 64
fi

: "${HADOOP_HOME:?HADOOP_HOME must point to the official Apache Hadoop 3.5.0 installation}"
hadoop_bin=${HADOOP_BIN:-"$HADOOP_HOME/bin/hadoop"}
hdfs_bin=${HDFS_BIN:-"$HADOOP_HOME/bin/hdfs"}
streaming_jar="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar"

if [[ ! -x "$hadoop_bin" || ! -x "$hdfs_bin" ]]; then
    printf 'Hadoop and HDFS executables must exist under HADOOP_HOME (or explicit overrides)\n' >&2
    exit 69
fi
if [[ ! -f "$streaming_jar" || "${streaming_jar##*/}" != hadoop-streaming-3.5.0.jar ]]; then
    printf 'official Apache Hadoop Streaming JAR is required exactly at %s\n' "$streaming_jar" >&2
    exit 69
fi
if [[ "$("$hadoop_bin" version | sed -n '1p')" != 'Hadoop 3.5.0' ]]; then
    printf 'Hadoop executable must report exactly Hadoop 3.5.0\n' >&2
    exit 69
fi

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/.." && pwd)
mapper="$repository_root/jobs/hadoop/mapper.py"
reducer="$repository_root/jobs/hadoop/reducer.py"
oracle_manifest="$repository_root/jobs/hadoop/oracle_manifest.py"
policy_config="$repository_root/configs/retention-policies.toml"
for shipped_file in "$mapper" "$reducer" "$oracle_manifest" "$policy_config"; do
    if [[ ! -f "$shipped_file" ]]; then
        printf 'required shipped file is missing: %s\n' "$shipped_file" >&2
        exit 66
    fi
done

events_source="$hdfs_input/events.jsonl"
snapshot_manifest_input="$hdfs_input/snapshot_manifest.json"
metrics_output="$hdfs_output/metrics/part-00000"
oracle_manifest_output="$hdfs_output/hadoop_oracle_manifest.json"
published_events="$hdfs_output/input/events.jsonl"
oracle_python=${PYTHON_BIN:-python3}
task_python=${HADOOP_TASK_PYTHON:-python3}
if [[ -z "$task_python" || "$task_python" == *$'\n'* || "$task_python" == *$'\r'* ]]; then
    printf 'HADOOP_TASK_PYTHON must be a non-empty single-line executable path\n' >&2
    exit 64
fi
printf -v task_python_command '%q' "$task_python"
gateway_pythonpath="$repository_root/src${PYTHONPATH:+:$PYTHONPATH}"
run_oracle_helper() {
    PYTHONPATH="$gateway_pythonpath" "$oracle_python" "$oracle_manifest" "$@"
}
temporary_directory=$(mktemp -d)
staging_root="${hdfs_output}.staging-$(date +%s)-$$"
publication_lock="${hdfs_output}.publication-lock"
hdfs_output_parent=${hdfs_output%/*}
if [[ -z "$hdfs_output_parent" ]]; then
    hdfs_output_parent=/
fi
staged_input_directory="$staging_root/input"
staged_events="$staged_input_directory/events.jsonl"
staged_metrics_directory="$staging_root/metrics"
staged_metrics="$staged_metrics_directory/part-00000"
staged_oracle_manifest="$staging_root/hadoop_oracle_manifest.json"
publication_lock_acquired=0
cleanup() {
    rm -rf -- "$temporary_directory"
    "$hdfs_bin" dfs -rm -r -f "$staging_root" >/dev/null 2>&1 || true
    if [[ $publication_lock_acquired -eq 1 ]]; then
        "$hdfs_bin" dfs -rm -f "$publication_lock" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT
local_snapshot_manifest="$temporary_directory/snapshot_manifest.json"
local_events="$temporary_directory/events.jsonl"
local_oracle_manifest="$temporary_directory/hadoop_oracle_manifest.json"
local_publication_lock="$temporary_directory/publication.lock"
printf '%s\n' "$$" > "$local_publication_lock"

"$hdfs_bin" dfs -mkdir -p "$hdfs_output_parent"
set +e
"$hdfs_bin" dfs -put "$local_publication_lock" "$publication_lock"
lock_status=$?
set -e
if [[ $lock_status -ne 0 ]]; then
    printf 'could not acquire HDFS publication lock: %s\n' "$publication_lock" >&2
    exit "$lock_status"
fi
publication_lock_acquired=1

set +e
"$hdfs_bin" dfs -test -e "$hdfs_output"
output_status=$?
set -e
if [[ $output_status -eq 0 ]]; then
    printf 'refusing to replace existing HDFS output: %s\n' "$hdfs_output" >&2
    exit 73
fi
if [[ $output_status -ne 1 ]]; then
    printf 'could not verify HDFS output absence: %s\n' "$hdfs_output" >&2
    exit "$output_status"
fi

"$hdfs_bin" dfs -get "$snapshot_manifest_input" "$local_snapshot_manifest"
"$hdfs_bin" dfs -get "$events_source" "$local_events"
run_oracle_helper verify-snapshot --snapshot-manifest "$local_snapshot_manifest" < "$local_events"
"$hdfs_bin" dfs -mkdir -p "$staged_input_directory"
"$hdfs_bin" dfs -put "$local_events" "$staged_events"
"$hdfs_bin" dfs -cat "$staged_events" | run_oracle_helper \
    verify-snapshot --snapshot-manifest "$local_snapshot_manifest"

streaming_args=(
    -D mapreduce.job.reduces=1
    -D mapreduce.job.name=tinyml-ids-hadoop-oracle-v1
    -files "$mapper,$reducer,$policy_config"
    -input "$staged_events"
    -output "$staged_metrics_directory"
    -mapper "$task_python_command mapper.py --policy-config retention-policies.toml"
    -reducer "$task_python_command reducer.py"
    -numReduceTasks 1
)

"$hadoop_bin" jar "$streaming_jar" "${streaming_args[@]}" >&2
"$hdfs_bin" dfs -cat "$staged_metrics" | \
    MSYS2_ARG_CONV_EXCL='--input-events-hdfs-path=;--output-metrics-hdfs-path=' \
    run_oracle_helper build \
    --snapshot-manifest "$local_snapshot_manifest" \
    --policy-config "$policy_config" \
    --input-events-hdfs-path="$published_events" \
    --output-metrics-hdfs-path="$metrics_output" > "$local_oracle_manifest"
"$hdfs_bin" dfs -put "$local_oracle_manifest" "$staged_oracle_manifest"
"$hdfs_bin" dfs -mv "$staging_root" "$hdfs_output"
printf '%s\n%s\n' "$metrics_output" "$oracle_manifest_output"
