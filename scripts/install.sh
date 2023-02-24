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
if [ "${TARGETPLATFORM:-}" = "linux/arm64" ]; then
    target="-Dtarget=aarch64-linux-gnu"
fi

set -eu

# Install Zig if it does not already exist:
if [ ! -d "zig" ]; then
    scripts/install_zig.sh
fi


if [ "$debug" = "true" ]; then
    echo "Building Tigerbeetle debug..."
    # shellcheck disable=SC2086
    zig/zig build install -Dcpu=baseline ${target}
else
    echo "Building TigerBeetle..."
    # shellcheck disable=SC2086
    zig/zig build install -Dcpu=baseline -Drelease-safe ${target}
fi
