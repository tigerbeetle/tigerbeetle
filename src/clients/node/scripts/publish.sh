#!/bin/sh
set -eu

echo "//registry.npmjs.org/:_authToken=${TIGERBEETLE_NODE_PUBLISH_KEY}" > ~/.npmrc
PACKAGE_JSON_VERSION=$(jq -r '.version' package.json)

exists="true"
npm show "tigerbeetle-node@${PACKAGE_JSON_VERSION}" --json 2>/dev/null || exists="false"

if [ "${exists}" = "true" ]; then
    echo "Package tigerbeetle-node@${PACKAGE_JSON_VERSION} already exists - not publishing"
else
    npm publish tigerbeetle-node-*.tgz
fi
