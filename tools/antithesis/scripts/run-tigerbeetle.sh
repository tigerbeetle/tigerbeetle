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

if ! find . -type f -name '*.tigerbeetle' | grep -q .
then ./tigerbeetle format \
	--cluster="$CLUSTER" \
	--replica="$REPLICA" \
	--replica-count="$REPLICA_COUNT" \
	0_0.antithesis.tigerbeetle
fi

exec ./tigerbeetle start \
	--addresses="$ADDRESSES" \
	0_0.antithesis.tigerbeetle

