#!/usr/bin/env bash
set -e

CWD=$(pwd)
COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

cd ./src/tigerbeetle
"$CWD"/zig/zig build -Drelease-safe
mv zig-out/bin/tigerbeetle "$CWD"
cd "$CWD"

REPLICAS="0"

function onerror {
    if [ "$?" == "0" ]; then
        rm benchmark.log
    else
        echo -e "${COLOR_RED}"
        echo "Error running benchmark, here are more details (from benchmark.log):"
        echo -e "${COLOR_END}"
        cat benchmark.log
    fi

    for I in $REPLICAS
    do
        echo "Stopping replica $I..."
    done
    kill %1
}
trap onerror EXIT

CLUSTER_ID="0000000000"
REPLICA_ADDRESSES="--addresses=3001"

echo "Initiating database file..."
mkdir -p benchmark
rm -f ./benchmark/cluster_${CLUSTER_ID}_replica_*.tigerbeetle
# Be careful to use a benchmark-specific filename so that we don't erase a real data file:
FILE="./benchmark/cluster_${CLUSTER_ID}_replica_0.tigerbeetle"
if [ -f $FILE ]; then
    rm $FILE
fi
./tigerbeetle format --cluster=$CLUSTER_ID --replica=0 $FILE

for I in $REPLICAS
do
    echo "Starting replica $I..."
    ./tigerbeetle start $REPLICA_ADDRESSES $FILE > benchmark.log 2>&1 &
done

# Wait for replicas to start, listen and connect:
sleep 1

echo ""
echo "Benchmarking..."
node dist/benchmark
echo ""
