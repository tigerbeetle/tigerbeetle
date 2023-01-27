#!/usr/bin/env sh

set -e

# macOS 13 Ventura is not supported on Zig 0.9.x.
# Overriding -target is one workaround Andrew suggests.
# https://github.com/ziglang/zig/issues/10478#issuecomment-1294313967
# Cut everything after the first `.` in the target query result
# because the rest of it doesn't always seem to be valid when passed
# back in to `-target`.
target="$(./zig/zig targets | grep triple |cut -d '"' -f 4 | cut -d '.' -f 1)"
if [ "$(./zig/zig targets | grep triple |cut -d '"' -f 4 | cut -d '.' -f 1,2)" = "aarch64-macos.13" ]; then
    target="native-macos.11"
fi

echo "Building for $target"

mkdir -p dist

./zig/zig build-lib \
	-mcpu=baseline \
	-OReleaseSafe \
	-dynamic \
	-lc \
	-isystem build/node-"$(node --version)"/include/node \
	-fallow-shlib-undefined \
	-femit-bin=dist/client.node \
	-target "$target" \
	src/node.zig 
