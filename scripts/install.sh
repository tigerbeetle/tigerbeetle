#!/bin/bash
set -eEuo pipefail

scripts/install_zig.sh
echo "Building TigerBeetle..."
if [[ "$DEBUG" == "true" ]]; then
    zig/zig build -Dcpu=baseline -Drelease-safe
else
    zig/zig build -Dcpu=baseline
fi
mv zig-out/bin/tigerbeetle .
