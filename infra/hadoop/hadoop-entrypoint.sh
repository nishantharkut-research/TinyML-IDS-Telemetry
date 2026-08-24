#!/usr/bin/env bash
set -euo pipefail

stop_services() {
    mapred --daemon stop historyserver >/dev/null 2>&1 || true
    yarn --daemon stop nodemanager >/dev/null 2>&1 || true
    yarn --daemon stop resourcemanager >/dev/null 2>&1 || true
    hdfs --daemon stop datanode >/dev/null 2>&1 || true
    hdfs --daemon stop namenode >/dev/null 2>&1 || true
}
trap stop_services EXIT
trap 'exit 0' INT TERM

wait_for_namenode() {
    local attempt
    for ((attempt = 1; attempt <= 30; attempt++)); do
        if hdfs dfsadmin -safemode get >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    printf 'NameNode RPC did not become ready within 60 seconds\n' >&2
    return 1
}

if [[ ! -f /opt/hadoop/data/namenode/current/VERSION ]]; then
    hdfs namenode -format -force -nonInteractive -clusterId tinyml-ids-telemetry
fi

hdfs --daemon start namenode
wait_for_namenode
hdfs --daemon start datanode
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /tmp /user/hadoop
hdfs dfs -chmod 1777 /tmp

yarn --daemon start resourcemanager
yarn --daemon start nodemanager
mapred --daemon start historyserver

required_processes=(
    org.apache.hadoop.hdfs.server.namenode.NameNode
    org.apache.hadoop.hdfs.server.datanode.DataNode
    org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
    org.apache.hadoop.yarn.server.nodemanager.NodeManager
    org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer
)

while true; do
    for process in "${required_processes[@]}"; do
        if ! pgrep -f "$process" >/dev/null; then
            printf 'required Hadoop process exited: %s\n' "$process" >&2
            exit 1
        fi
    done
    sleep 10
done
