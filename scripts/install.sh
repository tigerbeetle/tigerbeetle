#!/bin/bash
set -e
scripts/install_zig.sh
echo "Building TigerBeetle..."
zig/zig build -Drelease-safe
mv zig-out/bin/tigerbeetle .
