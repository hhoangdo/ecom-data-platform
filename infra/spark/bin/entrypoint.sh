#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-}"

if [[ -z "${ROLE}" ]]; then
  echo "Expected Spark role: master, worker, or history" >&2
  exit 1
fi

mkdir -p "${SPARK_HOME}/conf"

cat > "${SPARK_HOME}/conf/spark-env.sh" <<EOF
export JAVA_HOME=${JAVA_HOME}
export SPARK_MASTER_HOST=spark-master
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH=/workspace/src
EOF

cat > "${SPARK_HOME}/conf/spark-defaults.conf" <<EOF
spark.master spark://spark-master:7077
spark.app.name vina-bim-shop-batch
spark.sql.session.timeZone UTC
spark.driver.extraJavaOptions -Duser.timezone=UTC
spark.executor.extraJavaOptions -Duser.timezone=UTC
spark.sql.storeAssignmentPolicy ANSI
spark.sql.shuffle.partitions 8
spark.eventLog.enabled true
spark.eventLog.compress true
spark.eventLog.dir s3a://${VBS_CHECKPOINTS_BUCKET:-checkpoints}/spark-events
spark.history.fs.logDirectory s3a://${VBS_CHECKPOINTS_BUCKET:-checkpoints}/spark-events
spark.history.ui.port 18080
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type hive
spark.sql.catalog.iceberg.uri ${VBS_HIVE_METASTORE_INTERNAL_URI:-thrift://hive-metastore:9083}
spark.sql.catalog.iceberg.warehouse s3a://${VBS_SILVER_BUCKET:-silver}/warehouse
spark.sql.catalog.iceberg.io-impl org.apache.iceberg.hadoop.HadoopFileIO
spark.sql.catalog.iceberg.default-namespace silver
spark.sql.defaultCatalog iceberg
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint ${VBS_MINIO_INTERNAL_ENDPOINT:-http://minio:9000}
spark.hadoop.fs.s3a.endpoint.region ${VBS_MINIO_REGION:-us-east-1}
spark.hadoop.fs.s3a.access.key ${VBS_MINIO_ROOT_USER:-vina_minio}
spark.hadoop.fs.s3a.secret.key ${VBS_MINIO_ROOT_PASSWORD:-vina_minio_password}
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.executorEnv.PYTHONPATH /workspace/src
spark.yarn.appMasterEnv.PYTHONPATH /workspace/src
EOF

case "${ROLE}" in
  master)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host spark-master \
      --port 7077 \
      --webui-port 8080
    ;;
  worker)
    CORES="${SPARK_WORKER_CORES:-}"
    MEMORY="${SPARK_WORKER_MEMORY:-}"
    CORES_ARG=""
    MEMORY_ARG=""
    [ -n "${CORES}" ] && CORES_ARG="--cores ${CORES}"
    [ -n "${MEMORY}" ] && MEMORY_ARG="--memory ${MEMORY}"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --webui-port 8081 \
      ${CORES_ARG} ${MEMORY_ARG} \
      spark://spark-master:7077
    ;;
  history)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
    ;;
  *)
    echo "Unsupported Spark role: ${ROLE}" >&2
    exit 1
    ;;
esac
