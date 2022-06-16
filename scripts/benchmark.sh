#!/usr/bin/env bash
set -e

# Install Zig if it does not already exist:
if [ ! -d "zig" ]; then
    scripts/install_zig.sh
fi

COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

zig/zig build -Drelease-safe
mv zig-out/bin/tigerbeetle .

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

for I in 0
do
    echo "Formatting replica $I..."

    # Be careful to use a benchmark-specific filename so that we don't erase a real data file:
    FILE="./0_${I}.tigerbeetle.benchmark"
    if [ -f $FILE ]; then
        rm $FILE
    fi

    ./tigerbeetle format --cluster=0 --replica=$I $FILE > benchmark.log 2>&1
done

for I in 0
do
    echo "Starting replica $I..."
    FILE="./0_${I}.tigerbeetle.benchmark"
    ./tigerbeetle start --addresses=3001 $FILE > benchmark.log 2>&1 &
done

# Wait for replicas to start, listen and connect:
sleep 1

echo ""
echo "Benchmarking..."
zig/zig run -OReleaseSafe src/benchmark.zig
echo ""

for I in 0
do
    FILE="./0_${I}.tigerbeetle.benchmark"
    if [ -f $FILE ]; then
        rm $FILE
    fi
done
