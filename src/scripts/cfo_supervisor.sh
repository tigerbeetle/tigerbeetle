#!/bin/sh

# Scripts that runs `zig/zig build scripts -- cfo` in a loop.
# This is intentionally written in POSIX sh, as this is a bootstrap script that needs
# to be manually `scp`ed to the target machine.

set -eu

git --version

while true
do
    # Drop the cache every ~24 hours.
    if [ $((RANDOM % 24 )) -eq 0 ]
    then rm -rf ./tigerbeetle
    fi

    (
        if ! [ -d ./tigerbeetle ]
        then git clone https://github.com/tigerbeetle/tigerbeetle tigerbeetle
        fi

        cd tigerbeetle
        git fetch
        git switch --discard-changes --detach origin/main
        ./zig/download.sh
        unshare --user --pid --fork ./zig/zig build -Drelease scripts -- cfo
    ) || sleep 10 # Be resilient to cfo bugs and network errors, but avoid busy-loop retries.
done
