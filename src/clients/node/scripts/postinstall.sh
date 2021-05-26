#!/bin/bash
set -e

./src/tigerbeetle/scripts/install_zig.sh
./scripts/download_node_headers.sh
mkdir -p dist &&  zig/zig build-lib -dynamic -lc -isystem build/node-$(node --version)/include/node src/node.zig -femit-bin=dist/client.node