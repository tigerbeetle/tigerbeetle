#!/bin/bash
set -eEuo pipefail

scripts/install_zig.sh
echo "Building TigerBeetle..."
zig/zig build -Dcpu=baseline
mv zig-out/bin/tigerbeetle .
