#!/usr/bin/env bash

 set -e

docker run -v "$(pwd)/../../..":/wrk -w /wrk/src/clients/node --entrypoint bash node:lts -c "
npm config set cache /tmp --global
npm install
npm pack
"
