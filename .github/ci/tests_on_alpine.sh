#!/usr/bin/env bash

set -e

docker run --entrypoint sh -v "$(pwd)":/wrk -w /wrk alpine -c "
set -e
apk add -U git

# Deals with an error git throws within Docker when a git repo is
# volume mounted.:
#   fatal: detected dubious ownership in repository at '/wrk'
git config --global --add safe.directory /wrk

./scripts/install_zig.sh
./zig/zig build test
"
