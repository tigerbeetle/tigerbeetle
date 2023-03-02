#!/usr/bin/env bash
set -e

# Determine the operating system:
if [ "$(uname)" = "Linux" ]; then
    ZIG_OS="linux"
else
    ZIG_OS="macos"
fi

ZIG_EXE="./zig/zig"
BUILD_ROOT="./"
CACHE_ROOT="./zig-cache"
GLOBAL_CACHE_ROOT="$HOME/.cache/zig"

# This executes "zig/zig build {args}" using "-target native-{os}" for the build.zig executable.
# Using the {os} directly instead of "native" avoids hitting an abort on macos Ventura.
# TODO The abort was fixed in 0.10 while we're on 0.9. This script can be removed once we update zig.
$ZIG_EXE run ./scripts/build_runner.zig -target native-$ZIG_OS --main-pkg-path $BUILD_ROOT \
    -- $ZIG_EXE $BUILD_ROOT $CACHE_ROOT "$GLOBAL_CACHE_ROOT" \
    "$@"
