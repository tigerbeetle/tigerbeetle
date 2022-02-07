#!/bin/bash
set -e

./src/tigerbeetle/scripts/install_zig.sh
./scripts/download_node_headers.sh
mkdir -p dist && zig/zig build-lib -mcpu=baseline -OReleaseSafe -dynamic -lc -isystem build/node-$(node --version)/include/node src/node.zig -fallow-shlib-undefined -femit-bin=dist/client.node
