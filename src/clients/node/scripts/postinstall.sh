#!/bin/bash
set -e

./src/tigerbeetle/scripts/install_zig.sh 0.8.1
./scripts/download_node_headers.sh
mkdir -p dist && ZIG_SYSTEM_LINKER_HACK=1 zig/zig build-lib -mcpu=baseline -OReleaseSafe -dynamic -lc -isystem build/node-$(node --version)/include/node src/node.zig -fallow-shlib-undefined -femit-bin=dist/client.node
