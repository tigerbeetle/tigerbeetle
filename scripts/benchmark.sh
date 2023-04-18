#!/usr/bin/env bash
set -eEuo pipefail

# Number of replicas to benchmark
REPLICAS=${REPLICAS:-0}

# Install Zig if it does not already exist:
if [ ! -d "zig" ]; then
    scripts/install_zig.sh
fi

COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

# Determine the appropriate target flag (to support macos Ventura)
if [ "$(uname)" = "Linux" ]; then
    ZIG_TARGET="-Dtarget=native-linux"
else
    ZIG_TARGET="-Dtarget=native-macos"
fi

./scripts/build.sh install -Drelease-safe -Dconfig=production $ZIG_TARGET

function onerror {
    if [ "$?" == "0" ]; then
        echo "hi" #rm benchmark.log
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

    ./tigerbeetle format --cluster=0 --replica="$I" --replica-count=1 "$FILE" > benchmark.log 2>&1
done

for I in $REPLICAS
do
    echo "Starting replica $I..."
    FILE="./0_${I}.tigerbeetle.benchmark"
    ./tigerbeetle start --addresses=3001 "$FILE" >> benchmark.log 2>&1 &
done

echo ""
echo "Benchmarking..."
./scripts/build.sh benchmark -Drelease-safe -Dconfig=production $ZIG_TARGET -- "$@"
echo ""

for I in $REPLICAS
do
    FILE="./0_${I}.tigerbeetle.benchmark"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
done
