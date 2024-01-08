#!/bin/sh
set -eu

if ! test -e "${TIGERBEETLE_DATA_FILE}"; then
  /usr/local/bin/tigerbeetle format --cluster="${TIGERBEETLE_CLUSTER_ID}" --replica="${TIGERBEETLE_REPLICA_INDEX}" --replica-count="${TIGERBEETLE_REPLICA_COUNT}" "${TIGERBEETLE_DATA_FILE}"
fi
