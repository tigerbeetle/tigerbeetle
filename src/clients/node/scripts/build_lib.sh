#!/usr/bin/env sh

set -e

# This is a bit hacky. If the script is run inside tigerbeetle's repository,
# copy sources over, so that we can package them into an npm package.
#
# If the script is run inside user's node_modules, do nothing, as we assume
# the sources are already there.
if [ -f "../../tigerbeetle.zig" ] && [ -d "../../clients/node" ]; then
    rm -rf   ./src/tigerbeetle/src
    mkdir -p ./src/tigerbeetle/src
    cp -r ../../../scripts/ ./src/tigerbeetle/scripts/
    cp       ../../*.zig    ./src/tigerbeetle/src/
    for dir in io lsm testing vsr state_machine; do
        cp -r    ../../$dir     ./src/tigerbeetle/src/$dir
    done
fi

if ! [ -f "./zig/zig" ]; then
    ./src/tigerbeetle/scripts/install_zig.sh
fi

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

# Need to do string eval-ing because of shellcheck's strict string
# interpolation rules.
cmd="./zig/zig build-lib \
	--global-cache-dir ./zig/global-cache \
	-mcpu=baseline \
	-OReleaseSafe \
	-dynamic \
	-lc \
	-isystem build/node-$(node --version)/include/node \
	-fallow-shlib-undefined \
	-femit-bin=dist/client.node \
        $target src/node.zig"

eval "$cmd"
