#!/usr/bin/env sh
set -e

# Determine the operating system:
if [ "$(uname)" = "Linux" ]; then
    ZIG_OS="linux"
else
    ZIG_OS="macos"
fi

target_set="false"
case "$*" in
    *-Dtarget*)
	target_set="true"
	;;
esac

TARGET=""
if [ "$target_set" = "false" ]; then
    # Default to specifying "native-macos" if the target is not provided.
    # See https://github.com/ziglang/zig/issues/10478 (and note there's not a backport to 0.9.2).
    if [ "$(./zig/zig targets | grep '"os": "' | cut -d ":" -f 2 | cut -d '"' -f 2)" = "macos" ]; then
	TARGET="-Dtarget=native-macos"
    fi
fi
   
ZIG_EXE="./zig/zig"
BUILD_ROOT="./"
CACHE_ROOT="./zig-cache"
GLOBAL_CACHE_ROOT="$HOME/.cache/zig"

# This executes "zig/zig build {args}" using "-target native-{os}" for the build.zig executable.
# Using the {os} directly instead of "native" avoids hitting an abort on macos Ventura.
# TODO The abort was fixed in 0.10 while we're on 0.9. This script can be removed once we update zig.
$ZIG_EXE run ./scripts/build_runner.zig -target native-$ZIG_OS --main-pkg-path $BUILD_ROOT \
    -- $ZIG_EXE $BUILD_ROOT $CACHE_ROOT "$GLOBAL_CACHE_ROOT" $TARGET \
    "$@"
