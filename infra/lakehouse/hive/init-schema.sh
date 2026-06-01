#!/bin/sh
set -eu

export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G ${SERVICE_OPTS:-}"

if PGPASSWORD="${HIVE_METASTORE_PASSWORD}" \
  psql \
    --host lakehouse-postgres \
    --username "${HIVE_METASTORE_USER}" \
    --dbname "${HIVE_METASTORE_DB}" \
    --tuples-only \
    --no-align \
    --command "select 1 from information_schema.tables where table_schema = 'public' and table_name = 'VERSION'" \
  | grep -q '^1$'; then
  echo "Hive metastore schema already initialized."
  exit 0
fi

/opt/hive/bin/schematool -dbType "${DB_DRIVER:-postgres}" -initSchema
