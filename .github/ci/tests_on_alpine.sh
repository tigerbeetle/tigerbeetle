#!/usr/bin/env bash

set -e

docker run --entrypoint sh -v "$(pwd)":/wrk -w /wrk alpine -c "
set -e
./scripts/install_zig.sh
./zig/zig build test
"
