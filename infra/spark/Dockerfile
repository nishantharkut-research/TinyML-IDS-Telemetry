FROM apache/spark:4.1.3-python3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211 AS python-builder

USER root

ARG PYTHON_VERSION=3.11.16
ARG PYTHON_SHA256=91bcdebfdde239a003ae93738a7fce0f9230fee5c4bc2b86f6e6e8c6f98aabe8

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        libbz2-dev \
        libffi-dev \
        libgdbm-dev \
        liblzma-dev \
        libncursesw5-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        tk-dev \
        uuid-dev \
        xz-utils \
        zlib1g-dev \
    && curl --fail --location --show-error --silent \
        "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz" \
        --output /tmp/python.tar.xz \
    && echo "${PYTHON_SHA256}  /tmp/python.tar.xz" | sha256sum -c - \
    && mkdir /tmp/python-source \
    && tar --extract --xz --file /tmp/python.tar.xz --directory /tmp/python-source --strip-components=1 \
    && cd /tmp/python-source \
    && ./configure --prefix=/usr/local --enable-shared --with-ensurepip=install \
    && make -j2 \
    && make install \
    && rm -rf /var/lib/apt/lists/* /tmp/python-source /tmp/python.tar.xz

FROM apache/spark:4.1.3-python3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211

USER root
WORKDIR /workspace

ARG SPARK_KAFKA_CONNECTOR=org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3

ENV PATH=/opt/spark/bin:/opt/spark/sbin:/usr/local/bin:${PATH} \
    LD_LIBRARY_PATH=/usr/local/lib \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/workspace/src:/workspace

COPY --from=python-builder /usr/local /usr/local

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        libbz2-1.0 \
        libffi8 \
        libgdbm-compat4 \
        libgdbm6 \
        liblzma5 \
        libncursesw6 \
        libreadline8 \
        libsqlite3-0 \
        libssl3 \
        tk8.6 \
        uuid-runtime \
        zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && ldconfig \
    && python3 -c 'import ctypes, ssl, sys; assert sys.version_info[:3] == (3, 11, 16)'

RUN mkdir -p /opt/spark/conf /opt/spark/ivy \
    && printf '%s\n' \
        'from pyspark.sql import SparkSession' \
        'SparkSession.builder.master("local[1]").appName("connector-cache").getOrCreate().stop()' \
        > /tmp/cache_connector.py \
    && spark-submit \
        --conf spark.ui.enabled=false \
        --conf spark.jars.ivy=/opt/spark/ivy \
        --packages "$SPARK_KAFKA_CONNECTOR" \
        /tmp/cache_connector.py \
    && printf '\nspark.jars.ivy /opt/spark/ivy\nspark.sql.shuffle.partitions 4\n' \
        >> /opt/spark/conf/spark-defaults.conf \
    && rm /tmp/cache_connector.py \
    && chown -R 185:0 /opt/spark/ivy

COPY infra/container-constraints.txt ./infra/container-constraints.txt
COPY pyproject.toml README.md ./
COPY configs ./configs
COPY jobs ./jobs
COPY queries ./queries
COPY schemas ./schemas
COPY scripts ./scripts
COPY src ./src
COPY dashboard ./dashboard
COPY tests ./tests

RUN python3 -m pip install --no-cache-dir \
        --constraint infra/container-constraints.txt \
        -e ".[dev,kafka]" \
    && mkdir -p /home/spark/.ivy2 /workspace/artifacts \
    && chown -R 185:0 /home/spark /workspace \
    && chmod 0777 /home/spark/.ivy2 /workspace/artifacts

USER 185
ENTRYPOINT []
CMD ["sleep", "infinity"]
