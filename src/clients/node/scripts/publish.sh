#!/usr/bin/env bash
set -eu

PACKAGE_JSON_VERSION=$(jq -r '.version' package.json)
echo "//registry.npmjs.org/:_authToken=${TIGERBEETLE_NODE_PUBLISH_KEY}" > ~/.npmrc

exists="true"
npm show "tigerbeetle-node@${PACKAGE_JSON_VERSION}" --json 2>/dev/null || exists="false"

if [ "${exists}" = "true" ]; then
    echo "Package tigerbeetle-node@${PACKAGE_JSON_VERSION} already exists - did you not bump manually or run version_set.sh?"
    exit 1
else
    npm publish tigerbeetle-node-*.tgz
fi
