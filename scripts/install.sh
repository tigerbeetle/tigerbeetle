#!/bin/bash
set -e
scripts/install_zig.sh 0.8.1
echo "Building TigerBeetle..."
zig/zig build -Dcpu=baseline -Drelease-safe
mv zig-out/bin/tigerbeetle .
