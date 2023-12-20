#!/usr/bin/env sh
set -eu

# Repeatedly runs some zig build command with different seeds and checks that release and debug builds have indentical behaviour.

FUZZ_COMMAND=$1

while true; do
  SEED=$(od -A n -t u8 -N 8 /dev/urandom | xargs)
  zig build "$FUZZ_COMMAND" -Dhash-log-mode=create -Drelease -- --seed "$SEED" --events-max 100000;
  zig build "$FUZZ_COMMAND" -Dhash-log-mode=check -- --seed "$SEED" --events-max 100000;
done
