#!/usr/bin/env bash
set -e
zig build -Drelease-safe

CLUSTER_ID="--cluster-id=0a5ca1ab1ebee11e"
REPLICA_ADDRESSES="--replica-addresses=3001,3002"

for I in 0 1
do
    echo "Starting replica $I..."
    ./tigerbeetle $CLUSTER_ID $REPLICA_ADDRESSES --replica-index=$I > /dev/null 2>&1 &
done

sleep 2
echo ""
echo "Benchmarking..."
zig run -OReleaseSafe src/benchmark.zig
echo ""
sleep 1

for I in 0 1
do
    echo "Stopping replica $I..."
done
kill %1 %2
