#!/usr/bin/env bash
if [[ -n $ZIG_BUILD_TARGET ]]; then
    echo "Running zig build-lib, targeting $ZIG_BUILD_TARGET"
    mkdir -p dist && zig/zig build-lib -mcpu=baseline -OReleaseSafe -dynamic -lc -isystem build/node-$(node --version)/include/node src/node.zig -fallow-shlib-undefined -femit-bin=dist/client.node -target $ZIG_BUILD_TARGET
else
    mkdir -p dist && zig/zig build-lib -mcpu=baseline -OReleaseSafe -dynamic -lc -isystem build/node-$(node --version)/include/node src/node.zig -fallow-shlib-undefined -femit-bin=dist/client.node
fi