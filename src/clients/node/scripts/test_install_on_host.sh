#!/usr/bin/env bash
set -e

if [ -z ${SKIP_NODE_BUILD+x} ]; then
	./scripts/build.sh
fi

npm install

dir=$(pwd)
rm -rf /tmp/test-node-on-host && mkdir /tmp/test-node-on-host && cd /tmp/test-node-on-host
npm install "$dir"/tigerbeetle-node-*.tgz
node -e 'require("tigerbeetle-node"); console.log("SUCCESS!")'
