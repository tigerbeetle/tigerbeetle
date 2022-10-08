#!/usr/bin/env sh
set -eu

# Repeatedly runs some zig build command with different seeds and stores the output in the current directory.
# Eg `fuzz_repeatedly.sh fuzz_lsm_forest` will run `zig build fuzz_lsm_forest -- seed $SEED > fuzz_lsm_forest_fuzz_${SEED}`
# Use ./fuzz_unique_errors.sh to analyze the results.

FUZZ_COMMAND=$1

while true; do
  SEED=$(od -A n -t u8 -N 8 /dev/urandom | xargs)
  (zig build "$FUZZ_COMMAND" -- --seed "$SEED" 2>&1 || true) | tee "fuzz_${FUZZ_COMMAND}_${SEED}"
done
