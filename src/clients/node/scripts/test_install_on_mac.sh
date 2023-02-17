#!/usr/bin/env bash
set -e

npm install -g npm@latest
npm install
npm pack

dir=$(pwd)
mkdir /tmp/test && cd /tmp/test
npm install "$dir"/tigerbeetle-node-*.tgz
node -e 'require("tigerbeetle-node"); console.log("SUCCESS!")'
