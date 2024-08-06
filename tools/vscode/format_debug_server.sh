#!/usr/bin/env bash
set -eEuo pipefail
FILE=0_0.tigerbeetle.debug

# Download Zig if it does not yet exist:
if [ ! -f "zig/zig" ]; then
    zig/download.sh
fi

if [ -f "$FILE" ]; then
    rm $FILE
fi

./zig/zig build -Dconfig=production
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 $FILE
