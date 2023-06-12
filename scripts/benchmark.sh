#!/usr/bin/env bash
set -eEuo pipefail

zig build install -Drelease-safe -Dconfig=production

function onerror {
    kill %2
}
trap onerror EXIT

echo "Formatting replica..."

# Be careful to use a benchmark-specific filename so that we don't erase a real data file:
FILE="/tmp/0_0.tigerbeetle.benchmark"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi

./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 "$FILE"

tracy -a 127.0.0.1 &
sleep 0.1

PORT=$(python -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')

echo ""
echo "Benchmarking..."
./comp/benchmark --addresses ${PORT} "$@" &

echo "Starting replica..."
./tigerbeetle start --addresses=${PORT} "$FILE"
