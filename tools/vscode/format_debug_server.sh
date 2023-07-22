#!/usr/bin/env bash
set -eEuo pipefail
FILE=0_0.tigerbeetle.debug

# Install Zig if it does not already exist:
if [ ! -d "zig" ]; then
    scripts/install_zig.sh
fi

if [ -f "$FILE" ]; then
    rm $FILE
fi

./zig/zig build install -Dconfig=production
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 $FILE
