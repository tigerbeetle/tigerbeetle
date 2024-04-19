#!/bin/sh

# Scripts that runs `zig/zig build scripts -- cfo` in a loop.
# This is intentionally written in POSIX sh, as this is a bootstrap script that needs
# to be manually `scp`ed to the target machine.

set -eu

git --version

while true
do
    rm -rf ./tigerbeetle
    git clone https://github.com/tigerbeetle/tigerbeetle tigerbeetle
    cd tigerbeetle

    ./scripts/install_zig.sh

    # `unshare --pid` ensures that, if the parent process dies, all childrent die as well.
    # `unshare --user` is needed to make `--pid` work without root.
    # `|| true` because we want to be resilient to bugs in `cfo` itself. 
    unshare --user -f --pid \
        ./zig/zig build -Drelease scripts -- cfo \
        || true

    cd ../
done
