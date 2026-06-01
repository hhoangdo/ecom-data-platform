#!/bin/sh
set -eu

FLINK_API_URL="${VBS_FLINK_JOBMANAGER_INTERNAL_URL:-http://flink-jobmanager:8081}"

wait_for_jobmanager() {
  until curl -fsS "${FLINK_API_URL}/overview" >/dev/null 2>&1; do
    sleep 2
  done
}

submit_if_missing() {
  job_name="$1"
  job_script="$2"
  if curl -fsS "${FLINK_API_URL}/jobs/overview" | grep -q "\"name\":\"${job_name}\""; then
    return 0
  fi
  /opt/flink/bin/flink run -d -m flink-jobmanager:8081 \
    --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    -py "/workspace/scripts/flink/${job_script}"
}

wait_for_jobmanager
submit_if_missing "vina-bim-shop-commerce-metrics" "run_commerce_metrics_job.py"
submit_if_missing "vina-bim-shop-ops-alerts" "run_ops_alerts_job.py"
