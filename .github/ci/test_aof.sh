#!/usr/bin/env bash
set -eEuo pipefail

# Download Zig if it does not yet exist:
if [ ! -f "zig/zig" ]; then
    ./zig/download.sh
fi

./zig/zig build install -Dconfig-aof-recovery=true -Drelease
mv zig-out/bin/tigerbeetle tigerbeetle-aof-recovery

./zig/zig build install -Drelease

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

    kill $(jobs -p) 2> /dev/null
}
trap onerror EXIT

# Be careful to use a benchmark-specific filename so that we don't erase a real data file:
rm -f aof-test.tigerbeetle
rm -f aof-test.tigerbeetle.aof

./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 aof-test.tigerbeetle > aof.log 2>&1
./tigerbeetle start --cache-grid=256MiB --addresses=3000 --aof --experimental aof-test.tigerbeetle >> aof.log 2>&1 &

# Wait for replicas to start, listen and connect:
sleep 1

echo "Running benchmark to populate AOF..."
./tigerbeetle benchmark --addresses=3000 --transfer-count=400000 >> aof.log 2>&1

echo ""
echo "Running 'zig build aof -- debug aof-test.tigerbeetle.aof' to check AOF..."
data_checksum_src=$(./zig/zig build aof -- debug aof-test.tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Data checksum chain:')
echo "${data_checksum_src}"

echo ""
echo "Clearing cluster, and testing recovery"
kill %1
sleep 1

rm -rf 1 2

mkdir 1 && cd 1
../tigerbeetle-aof-recovery format --cluster=0 --replica=0 --replica-count=2 aof-test.tigerbeetle >> ../aof.log 2>&1
../tigerbeetle-aof-recovery start --cache-grid=256MiB --addresses=3001,3002 --aof-file="aof-test.tigerbeetle.aof" --experimental aof-test.tigerbeetle >> ../aof.log 2>&1 &
cd ..

mkdir 2 && cd 2
../tigerbeetle-aof-recovery format --cluster=0 --replica=1 --replica-count=2 aof-test.tigerbeetle >> ../aof.log 2>&1
../tigerbeetle-aof-recovery start --cache-grid=256MiB --addresses=3001,3002 --aof --experimental aof-test.tigerbeetle >> ../aof.log 2>&1 &
cd ..

# mkdir 3 && cd 3
# ../tigerbeetle-aof-recovery format --cluster=0 --replica=2 --replica-count=3 aof-test.tigerbeetle >> aof.log 2>&1
# ../tigerbeetle-aof-recovery start --addresses=3001,3002,3003 aof-test.tigerbeetle >> aof.log 2>&1 &
# cd ..

sleep 1

./zig/zig build aof -- recover 127.0.0.1:3001,127.0.0.1:3002 aof-test.tigerbeetle.aof >> aof.log 2>&1

# Give replicas time to settle.
sleep 10

echo ""
echo "Running 'zig build aof -- debug {1,2}/aof-test.tigerbeetle.aof' to check recovered AOF..."
data_checksum_recovered_1=$(./zig/zig build aof -- debug 1/aof-test.tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Data checksum chain:')
echo "1: ${data_checksum_recovered_1}"
data_checksum_recovered_2=$(./zig/zig build aof -- debug 2/aof-test.tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Data checksum chain:')
echo "2: ${data_checksum_recovered_2}"
# data_checksum_recovered_3=$(./zig/zig build aof -- debug 3/aof-test.tigerbeetle.aof 2>&1 | tee -a aof.log | grep 'Data checksum chain:')
# echo "3: ${data_checksum_recovered_3}"

if [ "${data_checksum_src}" != "${data_checksum_recovered_1}" ] || [ "${data_checksum_src}" != "${data_checksum_recovered_2}" ]; then
    echo "Mismatch in data checksums!"
    exit 1
fi

rm -rf 1 2
