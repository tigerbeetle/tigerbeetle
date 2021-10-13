#!/usr/bin/env bash

if [ "$1" == "state_machine" ]; then
    ./zig/zig test src/state_machine.zig
else
    ./zig/zig test src/unit_tests.zig
fi
