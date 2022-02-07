#!/bin/bash
set -e
scripts/install_zig.sh
echo "Building TigerBeetle..."
zig/zig build -Dcpu=baseline -Drelease-safe
mv zig-out/bin/tigerbeetle .
