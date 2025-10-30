#!/bin/sh

# Scripts that runs `zig/zig build scripts -- cfo` in a loop.
# This is intentionally written in POSIX sh, as this is a bootstrap script that needs
# to be manually `scp`ed to the target machine.

set -eu

# When the supervisor is killed or interrupted, kill all processes in the process group.
# (In particular, this will kill all descendent processes that have not changed groups.)
#
# We must unset the trap before killing, otherwise the signal will recurse and we segfault.
trap 'trap - INT TERM EXIT; kill 0' INT TERM EXIT

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
        # Run via `&`/`wait` rather than running directly, to ensure that it runs in the background,
        # but still allows signal processing, so that `kill`ing the supervisor doesn't just stall.
        ./zig/zig build scripts -- cfo &
        wait "$!"
    ) || sleep 10 # Be resilient to cfo bugs and network errors, but avoid busy-loop retries.
done
