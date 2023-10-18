#!/bin/sh -eu

usage() {
	cat <<-EOF
	usage: ${0##*/}

	Initialize and start a TigerBeetle replica.

	Required environment variables:
	  CLUSTER
	  REPLICA
	  REPLICA_COUNT
	  ADDRESSES
	EOF
}

if [ $# -ne 0 ] || [ -z "$CLUSTER" ] || [ -z "$REPLICA" ] || [ -z "$ADDRESSES" ] || [ -z "$REPLICA_COUNT" ]; then
	usage >&2
	exit 1
fi

datafile="${CLUSTER}_${REPLICA}.antithesis.tigerbeetle"

rm -f "${datafile}"

./tigerbeetle format \
	--cluster="$CLUSTER" \
	--replica="$REPLICA" \
	--replica-count="$REPLICA_COUNT" \
	"${datafile}"

exec ./tigerbeetle start \
	--addresses="$ADDRESSES" \
	"${datafile}"

