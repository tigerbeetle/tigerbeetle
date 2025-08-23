#!/usr/bin/env bash
set -euo pipefail

# Usage: ./bench.sh /dev/nvme6n1 output.log [runs]
DISK=${1:? "Usage: $0 /dev/<disk> output.log [runs]"}
LOGFILE=${2:? "Usage: $0 /dev/<disk> output.log [runs]"}
RUNS=5

ZIG=./zig/zig
TB=./tigerbeetle

# ⚠️ Destructive! We only zero the first 96 KiB, but it's still your disk.
zero_header() {
  sudo dd if=/dev/zero of="$DISK" bs=1K count=96 status=none
  sync
}

build() {
  "$ZIG" build --release -Dconfig_verify=false
}

bench_cmd() {
  local extra_flag=$1
    taskset -c 3-5 \
    "$TB" benchmark --cache-grid=16GiB --file="$DISK" $extra_flag
}

run_block() {
  local label=$1
  {
    echo
    echo "===== $(date -Is) : $label : DISK=$DISK ====="
    build
    zero_header
    bench_cmd "${2:-}"
  } 2>&1 | tee -a "$LOGFILE"
}

for i in $(seq 1 "$RUNS"); do
  run_block "normal associate (run $i)"
done

for i in $(seq 1 "$RUNS"); do
  run_block "zipfian associate (run $i)" "--account-distribution=zipfian"
done
