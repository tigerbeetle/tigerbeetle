#!/usr/bin/env bash
set -e

if [ -z ${SKIP_NODE_BUILD+x} ]; then
	./scripts/build.sh
fi

npm install -g npm@latest
npm install

dir=$(pwd)
mkdir /tmp/test && cd /tmp/test
npm install "$dir"/tigerbeetle-node-*.tgz
node -e 'require("tigerbeetle-node"); console.log("SUCCESS!")'
