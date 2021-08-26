#!/usr/bin/env bash
set -e
if [ "$1" ]; then
    echo "Running the simulator in debug mode with full debug logging enabled..."
    echo ""
    # TODO Be able to toggle the replay build mode.
    zig/zig run src/simulator.zig -ODebug -- $1
else
    for I in {1..1000}
    do
        zig/zig run src/simulator.zig -OReleaseSafe
    done
fi
