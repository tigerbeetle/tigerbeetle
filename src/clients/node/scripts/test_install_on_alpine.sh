#!/usr/bin/env bash

set -e

if [ -z ${SKIP_NODE_BUILD+x} ]; then
	./scripts/build.sh
fi

# Prebuild the container with this directory because we have no need for its artifacts
id=$(docker build -q -f - ../../.. < <(echo "
FROM alpine
COPY . /wrk"))

docker run -w /test "$id" sh -c "
set -e
apk add --update nodejs npm git
npm install /wrk/src/clients/node/tigerbeetle-node-*.tgz
node -e 'require(\"tigerbeetle-node\"); console.log(\"SUCCESS!\")'
"
