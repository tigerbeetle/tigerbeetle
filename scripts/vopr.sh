#!/usr/bin/env bash
set -e
for I in {1..100}
do
    zig/zig run src/simulator.zig -OReleaseSafe
done
