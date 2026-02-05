#!/usr/bin/env bash
set -eEuo pipefail

# Download Zig if it does not yet exist:
if [ ! -f "zig/zig" ]; then
    ./zig/download.sh
fi

./zig/zig build install 

# Be careful to use a benchmark-specific filenames so that we don't erase a real data file:
cleanup() {
    rm -f aof-test.tigerbeetle
    rm -f aof-test.tigerbeetle.aof
    rm -f aof.log
    rm -f {a1,a2,b1,b2}/aof-test.tigerbeetle{,.aof}
    rmdir {a1,a2,b1,b2} 2>/dev/null || true
}
cleanup

function onerror {
    if [ "$?" == "0" ]; then
        cleanup
    else
        echo
        echo "============================================================="
        echo "Error running aof test, here are more details (from aof.log):"
        echo "============================================================="
        cat aof.log
    fi

    kill $(jobs -p) 2> /dev/null || true
    wait
}
trap onerror EXIT

echo "Running benchmark to populate AOF..."
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 aof-test.tigerbeetle > aof.log 2>&1
./tigerbeetle start --cache-grid=256MiB --addresses=3000 --aof --experimental aof-test.tigerbeetle >> aof.log 2>&1 &
./tigerbeetle benchmark --addresses=3000 --transfer-count=400000 >> aof.log 2>&1
kill %1

echo ""
echo "Running 'zig build aof -- debug aof-test.tigerbeetle.aof' to check AOF..."
data_checksum_src=$(./zig/zig build aof -- debug aof-test.tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Data checksum chain:')
echo "${data_checksum_src}"

mkdir a1 a2 b1 b2

echo ''
echo 'Testing recovery...'
./tigerbeetle format --cluster=0 --replica=0 --replica-count=2 a1/aof-test.tigerbeetle >> ./aof.log 2>&1
./tigerbeetle start --aof-recovery --cache-grid=256MiB --addresses=3001,3002 --aof-file=a1/aof-test.tigerbeetle.aof --experimental a1/aof-test.tigerbeetle 2>&1 &
r1=$!
./tigerbeetle format --cluster=0 --replica=1 --replica-count=2 a2/aof-test.tigerbeetle >> ./aof.log 2>&1
./tigerbeetle start --aof-recovery --cache-grid=256MiB --addresses=3001,3002 --aof --experimental a2/aof-test.tigerbeetle 2>&1 &
r2=$!

sleep 1
./zig/zig build aof -- recover --cluster=0 --addresses=3001,3002 aof-test.tigerbeetle.aof >> aof.log 2>&1
sleep 10 # Give replicas time to settle.
kill $r1 $r2

echo ""
echo "Recovering a second time, to test determinism."

./tigerbeetle format --cluster=0 --replica=0 --replica-count=2 b1/aof-test.tigerbeetle >> ./aof.log 2>&1
./tigerbeetle start --aof-recovery --cache-grid=256MiB --addresses=3001,3002 --experimental b1/aof-test.tigerbeetle >> ./aof.log 2>&1 &
r1=$!
./tigerbeetle format --cluster=0 --replica=1 --replica-count=2 b2/aof-test.tigerbeetle >> ./aof.log 2>&1
./tigerbeetle start --aof-recovery --cache-grid=256MiB --addresses=3001,3002 --experimental b2/aof-test.tigerbeetle >> ./aof.log 2>&1 &
r2=$!

./zig/zig build aof -- recover --cluster=0 --addresses=3001,3002 aof-test.tigerbeetle.aof >> aof.log 2>&1
sleep 10 # Give replicas time to settle.
kill $r1 $r2

echo ""
echo "Running 'zig build aof -- debug a{1,2}/aof-test.tigerbeetle.aof' to check recovered AOF..."
data_checksum_recovered_1=$(./zig/zig build aof -- debug a1/aof-test.tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Data checksum chain:')
echo "1: ${data_checksum_recovered_1}"
data_checksum_recovered_2=$(./zig/zig build aof -- debug a2/aof-test.tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Data checksum chain:')
echo "2: ${data_checksum_recovered_2}"

if [ "${data_checksum_src}" != "${data_checksum_recovered_1}" ] || [ "${data_checksum_src}" != "${data_checksum_recovered_2}" ]; then
    echo "Mismatch in data checksums!"
    exit 1
fi

echo
echo 'Running "tigerbeetle inspect" to compare superblocks...'
superblock_a=$(./tigerbeetle inspect superblock ./a1/aof-test.tigerbeetle 2>/dev/null)
superblock_b=$(./tigerbeetle inspect superblock ./b1/aof-test.tigerbeetle 2>/dev/null)
if [ "$superblock_a" != "$superblock_b" ]; then
    echo "Mismatch in recovery determinism."
    exit 1
fi

echo
echo 'Success!'
