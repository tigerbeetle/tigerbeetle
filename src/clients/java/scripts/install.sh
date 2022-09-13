#!/bin/bash
set -eEuo pipefail

git submodule init
git submodule update

(cd ./src/zig/lib/tigerbeetle && scripts/install_zig.sh)
echo "Building TigerBeetle..."
(cd ./src/zig/lib/tigerbeetle && ./zig/zig build -Dcpu=baseline -Drelease-safe)
(cd ./src/zig/lib/tigerbeetle && mv ./zig-out/bin/tigerbeetle .)
echo "Building TigerBeetle JNI interface..."
(cd ./src/zig && ./lib/tigerbeetle/zig/zig build -Drelease-safe)