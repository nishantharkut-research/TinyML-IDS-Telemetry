FROM eclipse-temurin:17-jre-noble@sha256:5726444f65e67c407bac4ff1e036b475051f50bb3840273e81814e77c6fa8ee3

ARG HADOOP_SHA512=04ab94496cc00c8b7a28d03f6308eff8d2a4e7f37a9da5e8e086e4d6fc990e7a94d661908f6a6136039536efb362614b8aecdef185b5fb8ed588f0b152c7aa16

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace/src:/workspace

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl python3 python3-pip procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location --show-error \
        "https://downloads.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz" \
        --output /tmp/hadoop.tar.gz \
    && printf '%s  %s\n' "$HADOOP_SHA512" /tmp/hadoop.tar.gz | sha512sum --check --strict \
    && tar --extract --gzip --file /tmp/hadoop.tar.gz --directory /opt \
    && mv /opt/hadoop-3.5.0 "$HADOOP_HOME" \
    && rm /tmp/hadoop.tar.gz \
    && groupadd --gid 10001 hadoop \
    && useradd --uid 10001 --gid hadoop --create-home --shell /bin/bash hadoop

ENV HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_HDFS_HOME=/opt/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HADOOP_YARN_HOME=/opt/hadoop

WORKDIR /workspace
COPY infra/container-constraints.txt ./infra/container-constraints.txt
COPY pyproject.toml README.md ./
COPY configs ./configs
COPY jobs ./jobs
COPY queries ./queries
COPY schemas ./schemas
COPY scripts ./scripts
COPY src ./src
COPY tests ./tests
COPY infra/hadoop/conf/ /opt/hadoop/etc/hadoop/
COPY infra/hadoop/hadoop-entrypoint.sh /usr/local/bin/hadoop-entrypoint.sh

RUN python3 -m pip install --break-system-packages --no-cache-dir \
        --constraint infra/container-constraints.txt \
        -e ".[dev]" \
    && mkdir -p \
        /opt/hadoop/data/datanode \
        /opt/hadoop/data/namenode \
        /opt/hadoop/logs \
        /opt/hadoop/pids \
        /workspace/artifacts \
    && chown -R hadoop:hadoop /opt/hadoop/data /opt/hadoop/logs /opt/hadoop/pids /workspace /home/hadoop \
    && chmod 0755 /usr/local/bin/hadoop-entrypoint.sh \
    && chmod 0777 /workspace/artifacts

USER hadoop
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/hadoop-entrypoint.sh"]
