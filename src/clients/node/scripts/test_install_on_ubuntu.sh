#!/usr/bin/env bash

set -e

if [ -z ${SKIP_NODE_BUILD+x} ]; then
	./scripts/build.sh
fi

id=$(docker build -q -f - ../../.. < <(echo "
FROM ubuntu
COPY . /wrk"))
docker run -w /test "$id" sh -c "
set -e
apt-get update -y
apt-get install -y xz-utils wget git
wget -O- -q https://deb.nodesource.com/setup_18.x | bash -
apt-get install -y nodejs
npm install /wrk/src/clients/node/tigerbeetle-node-*.tgz
node -e 'require(\"tigerbeetle-node\"); console.log(\"SUCCESS!\")'
"
