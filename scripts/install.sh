#!/bin/sh

debug="$DEBUG"
if [[ "$1" == "--debug" ]]; then
    debug="true"
fi

set -eEuo pipefail

scripts/install_zig.sh
if [[ "$debug" == "true" ]]; then
    echo "Building Tigerbeetle debug..."
    zig/zig build -Dcpu=baseline
else
    echo "Building TigerBeetle..."
    zig/zig build -Dcpu=baseline -Drelease-safe
fi
mv zig-out/bin/tigerbeetle .
