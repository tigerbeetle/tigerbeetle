#!/usr/bin/env bash
set -eu -o pipefail

NPM_LATEST_VERSION=$(npm show tigerbeetle-node --json | jq -r '."dist-tags".latest')
PACKAGE_JSON_VERSION=$(jq -r '.version' package.json)

PATCH_VERSION_PLUS_ONE=$(("$(echo "${NPM_LATEST_VERSION}" | cut -d "." -f 3)" + 1))
NPM_PLUS_ONE_VERSION="$(echo "${NPM_LATEST_VERSION}" | cut -d "." -f 1-2).${PATCH_VERSION_PLUS_ONE}"

exists="true"
npm show "tigerbeetle-node@${PACKAGE_JSON_VERSION}" --json >/dev/null 2>/dev/null || exists="false"
if [ "${exists}" = "true" ]; then
    echo "Package tigerbeetle-node@${PACKAGE_JSON_VERSION} already exists - incrementing patch version from latest on npm (${NPM_LATEST_VERSION}) to (${NPM_PLUS_ONE_VERSION})"

    jq ".version=\"${NPM_PLUS_ONE_VERSION}\"" package.json > package.json-new
    mv -f package.json-new package.json
else
    echo "Package tigerbeetle-node@${PACKAGE_JSON_VERSION} doens't exists - no change made"
fi