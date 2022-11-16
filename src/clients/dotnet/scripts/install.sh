#!/bin/bash
set -eEuo pipefail

git submodule init
git submodule update

(cd ./tigerbeetle && scripts/install_zig.sh)
echo "Building TigerBeetle..."
(cd ./tigerbeetle && ./zig/zig build -Dcpu=baseline -Drelease-safe)
(cd ./tigerbeetle && mv ./zig-out/bin/tigerbeetle .)