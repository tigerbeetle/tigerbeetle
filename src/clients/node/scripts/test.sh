#!/usr/bin/env bash
set -e

CWD=$(pwd)
COLOR_RED='\033[1;31m'
COLOR_END='\033[0m'

cd ../../..
./zig/zig build install -Doptimize=ReleaseSafe
TIGERBEETLE_EXE=$(readlink -f ./tigerbeetle)
cd "$CWD"

REPLICAS="0"

function onerror {
    if [ "$?" == "0" ]; then
        rm tigerbeetle_test.log
    else
        echo -e "${COLOR_RED}"
        echo "Error running test, here are more details (from tigerbeetle_test.log):"
        echo -e "${COLOR_END}"
        cat tigerbeetle_test.log
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
mkdir -p test
rm -f ./test/cluster_${CLUSTER_ID}_replica_*.tigerbeetle
# Be careful to use a unit-test-specific filename so that we don't erase a real data file:
FILE="./test/cluster_${CLUSTER_ID}_replica_0.tigerbeetle"
if [ -f $FILE ]; then
    rm $FILE
fi
$TIGERBEETLE_EXE format --cluster=$CLUSTER_ID --replica=0 --replica-count=1 $FILE

for I in $REPLICAS
do
    echo "Starting replica $I..."
    $TIGERBEETLE_EXE start $REPLICA_ADDRESSES $FILE > tigerbeetle_test.log 2>&1 &
done

# Wait for replicas to start, listen and connect:
sleep 1

echo ""
echo "Running unit tests..."
(cd "$CWD" && node dist/test)
echo ""
