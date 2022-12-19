#!/usr/bin/env bash
set -eEuo pipefail

echo "Building TigerBeetle..."
(cd ../../../ && ./zig/zig build install -Dcpu=baseline -Drelease-safe)

echo "Formatting replica ..."

# Be careful to use a benchmark-specific filename so that we don't erase a real data file:
FILE="./0_0.tigerbeetle.examples"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi

../../../tigerbeetle format --cluster=0 --replica=0 "$FILE"

echo "Starting tigerbeetle ..."
FILE="./0_0.tigerbeetle.examples"
../../../tigerbeetle start --addresses=3000 "$FILE"
