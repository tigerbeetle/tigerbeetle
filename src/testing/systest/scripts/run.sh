#!/bin/sh -eu

usage() {
  echo "usage: ${0##*/}"
  echo ""
  echo "Initialize and start a TigerBeetle replica."

  echo "Required environment variables:"
  echo "  CLUSTER"
  echo "  REPLICA"
  echo "  REPLICA_COUNT"
  echo "  ADDRESSES"
}

if [ $# -ne 0 ] || [ -z "$CLUSTER" ] || [ -z "$REPLICA" ] || [ -z "$ADDRESSES" ] || [ -z "$REPLICA_COUNT" ]; then
  usage >&2
  exit 1
fi

datafile="/var/data/${CLUSTER}_${REPLICA}.antithesis.tigerbeetle"

if [ ! -f "${datafile}" ]; then
  ./tigerbeetle format \
    --cluster="$CLUSTER" \
    --replica="$REPLICA" \
    --replica-count="$REPLICA_COUNT" \
    "${datafile}"
fi

exec ./tigerbeetle start \
  --addresses="$ADDRESSES" \
  "${datafile}"

