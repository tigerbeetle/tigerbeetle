#!/usr/bin/env bash

 set -e

docker run -v "$(pwd)":/wrk -w /wrk --entrypoint bash node -c "
npm config set cache /tmp --global
npm install
npm pack
"
