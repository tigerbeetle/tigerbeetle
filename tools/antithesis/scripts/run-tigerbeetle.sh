#!/bin/sh -eu

usage() {
	cat <<-EOF
	usage: ${0##*/}

	Initialize and start a TigerBeetle replica.

	Required environment variables:
	  CLUSTER
	  REPLICA
	  ADDRESSES
	EOF
}

if [ $# -ne 0 ] || [ -z "$CLUSTER" ] || [ -z "$REPLICA" ] || [ -z "$ADDRESSES" ]; then
	usage >&2
	exit 1
fi

directory='.'

if ! find . -type f -name '*.tigerbeetle' | grep -q .
then ./tigerbeetle init \
	--directory="$directory" \
	--cluster="$CLUSTER" \
	--replica="$REPLICA"
fi

exec ./tigerbeetle start \
	--directory="$directory" \
	--cluster="$CLUSTER" \
	--replica="$REPLICA" \
	--addresses="$ADDRESSES"
