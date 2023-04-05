#!/usr/bin/env bash
set -eEuo pipefail

# Install Zig if it does not already exist:
if [ ! -d "zig" ]; then
    scripts/install_zig.sh
fi

zig/zig build -Daof=true
mv zig-out/bin/tigerbeetle tigerbeetle-aof

zig/zig build -Daof=true -Daof_recovery=true
mv zig-out/bin/tigerbeetle tigerbeetle-aof-recovery

rm -f aof.log

function onerror {
    if [ "$?" == "0" ]; then
        rm aof.log
    else
        echo
        echo "============================================================="
        echo "Error running aof test, here are more details (from aof.log):"
        echo "============================================================="
        cat aof.log
    fi

    kill %1
}
trap onerror EXIT


# Be careful to use a benchmark-specific filename so that we don't erase a real data file:
FILE="./0_0.tigerbeetle.aof-test"
if [ -f "$FILE" ]; then
    rm "$FILE"
fi

if [ -f "./tigerbeetle.aof" ]; then
    echo "tigerbeetle.aof already exists. Please delete it and try again."
    exit 1
fi

./tigerbeetle-aof format --cluster=0 --replica=0 --replica-count=1 "$FILE" > aof.log 2>&1
./tigerbeetle-aof start --addresses=3001 "$FILE" >> aof.log 2>&1 &

# Wait for replicas to start, listen and connect:
sleep 1

echo "Running 'zig build benchmark' to populate AOF..."
zig/zig build benchmark -- --transfer-count 400000 >> aof.log 2>&1

echo ""
echo "Running 'zig build aof -- debug tigerbeetle.aof' to check AOF..."
body_checksum_src=$(zig/zig build aof -- debug tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Body checksum chain:')
echo "${body_checksum_src}"

echo ""
echo "Clearing cluster, and testing recovery"
kill %1
sleep 1
rm "$FILE"
mv tigerbeetle.aof tigerbeetle.aof-src
./tigerbeetle-aof-recovery format --cluster=0 --replica=0 --replica-count=1 "$FILE" >> aof.log 2>&1
./tigerbeetle-aof-recovery start --addresses=3001 "$FILE" >> aof.log 2>&1 &
sleep 1

zig/zig build aof -- recover 127.0.0.1:3001 tigerbeetle.aof-src >> aof.log 2>&1

echo ""
echo "Running 'zig build aof -- debug tigerbeetle.aof' to check recovered AOF..."
body_checksum_recovered=$(zig/zig build aof -- debug tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Body checksum chain:')
echo "${body_checksum_recovered}"

if [ "${body_checksum_src}" != "${body_checksum_recovered}" ]; then
    echo "Mismatch in body checksums!"
    exit 1
fi

rm tigerbeetle.aof
rm tigerbeetle.aof-src

 # TODO: Check timestamp is set correctly somehow