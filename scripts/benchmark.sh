#!/usr/bin/env bash
set -eEuo pipefail

# Number of replicas to benchmark
REPLICAS=${REPLICAS:-0}

# Install Zig if it does not already exist:
if [ ! -d "zig" ]; then
    scripts/install_zig.sh
fi

PORT=3001
if command -v python &> /dev/null; then
    PORT=$(python -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
elif command -v python3 &> /dev/null; then
    PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
fi

COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

# Determine the appropriate target flag (to support macos Ventura)
if [ "$(uname)" = "Linux" ]; then
    ZIG_TARGET="-Dtarget=native-linux"
else
    ZIG_TARGET="-Dtarget=native-macos"
fi

# shellcheck disable=SC2086
zig/zig build install -Drelease -Dconfig=production $ZIG_TARGET

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

    ./tigerbeetle format --cluster=0 --replica="$I" --replica-count=1 "$FILE" > benchmark.log 2>&1
done

for I in $REPLICAS
do
    echo "Starting replica $I..."
    FILE="./0_${I}.tigerbeetle.benchmark"
    ./tigerbeetle start "--addresses=${PORT}" "$FILE" >> benchmark.log 2>&1 &
done

echo ""
echo "Benchmarking..."
# shellcheck disable=SC2086
zig/zig build benchmark -Drelease -Dconfig=production $ZIG_TARGET -- --addresses="${PORT}" "$@"
echo ""

for I in $REPLICAS
do
    FILE="./0_${I}.tigerbeetle.benchmark"
    if [ -f "$FILE" ]; then
        rm "$FILE"
    fi
done
