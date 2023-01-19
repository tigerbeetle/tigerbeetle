#!/usr/bin/env bash

set -e

# macOS 13 Ventura is not supported on Zig 0.9.x.
# Overriding -target is one workaround Andrew suggests.
# https://github.com/ziglang/zig/issues/10478#issuecomment-1294313967
# Cut everything after the second `.` in the target query result
# because the rest of it doesn't always seem to be valid when passed
# back in to `-target`.
target="$(zig targets | jq -r '.native.triple' | cut -d '.' -f 1,2)"
if [[ "$target" == "aarch64-macos.13" ]]; then
    target="native-macos.11"
fi

echo "Building for $target"

zig/zig build-lib \
	-mcpu=baseline \
	-OReleaseSafe \
	-dynamic \
	-lc \
	-isystem build/node-"$(node --version)"/include/node \
	-fallow-shlib-undefined \
	-femit-bin=dist/client.node \
	-target "$target" \
	src/node.zig 
