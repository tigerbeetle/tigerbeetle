#!/bin/sh
set -eu

echo "//registry.npmjs.org/:_authToken=${TIGERBEETLE_NODE_PUBLISH_KEY}" > ~/.npmrc

NPM_LATEST_VERSION=$(npm show tigerbeetle-node --json | jq -r '."dist-tags".latest')
PACKAGE_JSON_VERSION=$(jq -r '.version' package.json)

exists="true"
npm show "tigerbeetle-node@${PACKAGE_JSON_VERSION}" --json 2>/dev/null || exists="false"

if [ "${exists}" = "true" ]; then
    PATCH_VERSION_PLUS_ONE=$(("$(echo "${NPM_LATEST_VERSION}" | cut -d "." -f 3)" + 1))
    NPM_PLUS_ONE_VERSION="$(echo "${NPM_LATEST_VERSION}" | cut -d "." -f 1-2).${PATCH_VERSION_PLUS_ONE}"
    echo "Package tigerbeetle-node@${PACKAGE_JSON_VERSION} already exists - incrementing patch version from latest on npm (${NPM_LATEST_VERSION}) to (${NPM_PLUS_ONE_VERSION})"

    rm -rf /tmp/change-package-version && mkdir -p /tmp/change-package-version
    tar -xzpf tigerbeetle-node-*.tgz -C /tmp/change-package-version
    jq ".version=\"${NPM_PLUS_ONE_VERSION}\"" /tmp/change-package-version/package/package.json > /tmp/change-package-version/package/package.json-new
    jq ".version=\"${NPM_PLUS_ONE_VERSION}\" | .packages[\"\"].version=\"${NPM_PLUS_ONE_VERSION}\"" /tmp/change-package-version/package/package-lock.json > /tmp/change-package-version/package/package-lock.json-new

    mv -f /tmp/change-package-version/package/package.json-new /tmp/change-package-version/package/package.json
    mv -f /tmp/change-package-version/package/package-lock.json-new /tmp/change-package-version/package/package-lock.json

    tar -czpf tigerbeetle-node-updated.tgz -C /tmp/change-package-version .

    npm publish tigerbeetle-node-updated.tgz
else
    npm publish tigerbeetle-node-*.tgz
fi
