FROM flink:1.19.2-scala_2.12-java17

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-pip curl ca-certificates coreutils \
  && ln -sf /usr/bin/python3 /usr/local/bin/python \
  && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir \
  apache-flink==1.19.2 \
  confluent-kafka>=2.8 \
  minio>=7.2 \
  pandas>=2.2 \
  pyyaml>=6.0 \
  requests>=2.32

RUN mkdir -p /opt/flink/usrlib /opt/flink/plugins/s3-fs-hadoop

RUN curl -fsSL -o /opt/flink/usrlib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar \
  && curl -fsSL -o /opt/flink/plugins/s3-fs-hadoop/s3-fs-hadoop-1.19.2.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.19.2/flink-s3-fs-hadoop-1.19.2.jar

COPY infra/flink/bin/submit-jobs.sh /opt/flink/bin/submit-jobs.sh
COPY infra/flink/bin/run-with-time-limit.sh /opt/flink/bin/run-with-time-limit.sh
RUN chmod +x /opt/flink/bin/submit-jobs.sh /opt/flink/bin/run-with-time-limit.sh

ENV PYTHONPATH=/workspace/src

USER flink
