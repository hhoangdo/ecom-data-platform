#!/bin/sh
set -eu

MAX_RUNTIME_MINUTES="${VBS_FLINK_MAX_RUNTIME_MINUTES:-45}"
GRACE_SECONDS="${VBS_FLINK_MAX_RUNTIME_GRACE_SECONDS:-30}"
DISABLE_AUTO_STOP="$(printf '%s' "${VBS_FLINK_DISABLE_AUTO_STOP:-false}" | tr '[:upper:]' '[:lower:]')"

if [ "${DISABLE_AUTO_STOP}" = "true" ] || [ "${MAX_RUNTIME_MINUTES}" = "0" ]; then
  exec "$@"
fi

exec timeout --signal=TERM --kill-after="${GRACE_SECONDS}s" "${MAX_RUNTIME_MINUTES}m" "$@"
