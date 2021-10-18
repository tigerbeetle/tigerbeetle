#!/usr/bin/env bash

if [ "$1" != "" ]; then
    ./zig/zig test src/$1.zig
else
    ./zig/zig test src/unit_tests.zig
fi
