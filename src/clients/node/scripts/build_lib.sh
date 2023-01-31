#!/usr/bin/env sh

set -e

# macOS 13 Ventura is not supported on Zig 0.9.x.
# Overriding -target is one workaround Andrew suggests.
# https://github.com/ziglang/zig/issues/10478#issuecomment-1294313967
target=""
if [ "$(./zig/zig targets | grep triple |cut -d '"' -f 4 | cut -d '.' -f 1,2)" = "aarch64-macos.13" ]; then
    target="-target native-macos.11"
fi

# Zig picks musl libc on RHEL instead of glibc, incorrectly
# https://github.com/ziglang/zig/issues/12156
if [ -f "/etc/redhat-release" ]; then
   if ! grep Fedora /etc/redhat-release; then
       target="-target native-native-gnu"
   fi
fi

if [ "$target" = "" ]; then
    echo "Building default target"
else
    echo "Building for '$target'"
fi

mkdir -p dist

# Need to do string eval-ing because of shellcheck's strict string
# interpolation rules.
cmd="./zig/zig build-lib \
	-mcpu=baseline \
	-OReleaseSafe \
	-dynamic \
	-lc \
	-isystem build/node-$(node --version)/include/node \
	-fallow-shlib-undefined \
	-femit-bin=dist/client.node \
        $target src/node.zig"

eval "$cmd"
