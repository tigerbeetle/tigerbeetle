#!/usr/bin/env bash

set -e

docker run --entrypoint sh -v "$(pwd)":/wrk -w /wrk fedora -c "
set -e
dnf update -y && dnf install xz -y
./scripts/install_zig.sh
zig/zig build test
"
