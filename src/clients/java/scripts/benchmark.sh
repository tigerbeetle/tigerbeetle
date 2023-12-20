#!/usr/bin/env bash
set -eEuo pipefail

# Number of replicas to benchmark
REPLICAS=${REPLICAS:-0}

COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

echo "Building TigerBeetle..."
(cd ../../.. && ./zig/zig build install -Drelease)

echo "Building TigerBeetle Java Client"
mvn -B compile --quiet

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

for I in $REPLICAS
do
    echo "Formatting replica $I..."

    # Be careful to use a benchmark-specific filename so that we don't erase a real data file:
    FILE="./0_${I}.tigerbeetle.benchmark"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi

    ../../../tigerbeetle format --cluster=0 --replica="$I" --replica-count=1 "$FILE" > benchmark.log 2>&1
done

for I in $REPLICAS
do
    echo "Starting replica $I..."
    FILE="./0_${I}.tigerbeetle.benchmark"
    ../../../tigerbeetle start --addresses=3001 "$FILE" > benchmark.log 2>&1 &
done

# Wait for replicas to start, listen and connect:
sleep 1

echo ""
echo "Benchmarking..."
java -cp ./target/classes benchmark/Benchmark
echo ""

for I in $REPLICAS
do
    FILE="./0_${I}.tigerbeetle.benchmark"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
done
