#!/usr/bin/env bash

set -e

docker run --entrypoint sh -v "$(pwd)":/wrk -w /wrk ubuntu -c "
set -e

apt-get update -y
apt-get install -y curl xz-utils

./scripts/install_zig.sh
zig/zig build test
./scripts/install.sh
"
