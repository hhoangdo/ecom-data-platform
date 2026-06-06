#!/bin/sh
set -eu

mc alias set ALIAS http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb --ignore-existing ALIAS/bronze
mc mb --ignore-existing ALIAS/silver
mc mb --ignore-existing ALIAS/gold
mc mb --ignore-existing ALIAS/checkpoints
mc mb --ignore-existing ALIAS/evidence

mc pipe ALIAS/checkpoints/spark-events/.keep </dev/null

mc ls ALIAS
