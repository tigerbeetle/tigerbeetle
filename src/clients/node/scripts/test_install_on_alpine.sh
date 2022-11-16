#!/usr/bin/env bash

set -e

./scripts/build.sh

# Prebuild the container with this directory because we have no need for its artifacts
id=$(docker build -q -f - . < <(echo "
FROM alpine
COPY . /wrk"))

docker run -w /test "$id" sh -c "
set -e
apk add --update nodejs npm git

npm install /wrk
node -e 'require(\"tigerbeetle-node\"); console.log(\"SUCCESS!\")'
"
