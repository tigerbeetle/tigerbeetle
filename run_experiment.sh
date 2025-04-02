#!/bin/bash
for (( i=4; i<=4096; i*=2 )); do
  echo "$i"
  dd if=/dev/zero of=/dev/nvme2n1 bs=1K count=96
  numactl -C 3-5 ./tigerbeetle benchmark --cache-grid=32GiB --file=/dev/nvme2n1 --query-count=0 --transfer-batch-size=$i &>> experiment_optimized.log
done
