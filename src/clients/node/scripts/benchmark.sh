#!/usr/bin/env bash
set -e

CWD=$(pwd)
COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

yarn
yarn build

cd ./src/tigerbeetle
$CWD/zig/zig build -Drelease-safe
mv zig-out/bin/tigerbeetle $CWD
cd $CWD

function onerror {
    if [ "$?" == "0" ]; then
        rm benchmark.log
    else
        echo -e "${COLOR_RED}"
        echo "Error running benchmark, here are more details (from benchmark.log):"
        echo -e "${COLOR_END}"
        cat benchmark.log
    fi

    for I in 0
    do
        echo "Stopping replica $I..."
    done
    kill %1
}
trap onerror EXIT

CLUSTER_ID="--cluster=0"
REPLICA_ADDRESSES="--addresses=3001"

# ./tigerbeetle init --cluster=0 --replica=0 --directory=.

for I in 0
do
    echo "Starting replica $I..."
    ./tigerbeetle start $CLUSTER_ID $REPLICA_ADDRESSES --replica=$I --directory=. > benchmark.log 2>&1 &
done

# Wait for replicas to start, listen and connect:
sleep 1

echo ""
echo "Benchmarking..."
node dist/benchmark
echo ""
