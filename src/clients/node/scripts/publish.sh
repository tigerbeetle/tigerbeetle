#!/usr/bin/env bash
set -eu

echo "//registry.npmjs.org/:_authToken=${TIGERBEETLE_NODE_PUBLISH_KEY}" > ~/.npmrc
npm publish tigerbeetle-node-*.tgz
