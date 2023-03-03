#!/usr/bin/env sh
# TODO: In future, it would be great to have `zig build install` do everything,
# with the user just needing to run ./scripts/install_zig.sh; zig/zig build install
if [ -z "$DEBUG" ]; then
    debug="$DEBUG"
fi

debug="$DEBUG"
if [ "$1" = "--debug" ]; then
    debug="true"
fi

target="${TARGET:-}"
if [ -n "${target}" ]; then
    target="-Dtarget=${target}"
fi

if [ "${TARGETPLATFORM:-}" = "linux/arm64" ]; then
    target="-Dtarget=aarch64-linux-gnu"
fi

set -eu

# Install Zig if it does not already exist:
if [ ! -d "zig" ]; then
    scripts/install_zig.sh
fi

# Default to specifying "native-macos" if the target is not provided:
if [ "$target" = "" ]; then
    if [ "$(./zig/zig targets | grep '"os": "' | cut -d ":" -f 2 | cut -d '"' -f 2)" = "macos" ]; then
       target="-Dtarget=native-macos"
    fi
fi

if [ "$debug" = "true" ]; then
    echo "Building Tigerbeetle debug..."
    ./scripts/build.sh install -Dcpu=baseline "${target}"
else
    echo "Building TigerBeetle..."
    ./scripts/build.sh install -Dcpu=baseline -Drelease-safe "${target}"
fi
