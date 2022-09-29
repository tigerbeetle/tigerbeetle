#!/bin/bash

debug="$DEBUG"
if [[ "$1" == "--debug" ]]; then
    debug="true"
fi

set -eEuo pipefail

scripts/install_zig.sh
echo "Building TigerBeetle..."
if [[ "$debug" == "true" ]]; then
    zig/zig build -Dcpu=baseline
else
    zig/zig build -Dcpu=baseline -Drelease-safe
fi
mv zig-out/bin/tigerbeetle .
