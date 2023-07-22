#!/usr/bin/env bash

set -e

docker run --security-opt seccomp=unconfined -e COV --entrypoint sh -v "$(pwd)":/wrk -w /wrk ubuntu -c "
set -e

apt-get update -y
apt-get install -y curl xz-utils

if [ -n \"\${COV}\" ]; then
	apt-get install -y kcov
fi

./scripts/install_zig.sh
./zig/zig build test
"
