FROM apache/spark:4.0.0

ARG ICEBERG_VERSION=1.10.1
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_BUNDLE_VERSION=2.24.6

USER root

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=/opt/spark/bin:/opt/spark/sbin:/opt/java/openjdk/bin:${PATH}

RUN curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" \
    -o "${SPARK_HOME}/jars/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" \
  && curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
  && curl -fsSL \
    "https://repo.maven.apache.org/maven2/software/amazon/awssdk/bundle/${AWS_BUNDLE_VERSION}/bundle-${AWS_BUNDLE_VERSION}.jar" \
    -o "${SPARK_HOME}/jars/bundle-${AWS_BUNDLE_VERSION}.jar"

RUN python3 -m pip install --no-cache-dir \
  "great-expectations==1.17.2" \
  "pandas==2.2.3" \
  "pyarrow==19.0.1" \
  "PyYAML==6.0.2"

RUN mkdir -p /opt/vina /workspace \
  && ln -sf /usr/bin/python3 /usr/local/bin/python \
  && chown -R spark:spark /opt/vina /workspace

COPY infra/spark/bin/entrypoint.sh /opt/vina/entrypoint.sh

RUN chmod +x /opt/vina/entrypoint.sh

USER spark

WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/vina/entrypoint.sh"]
