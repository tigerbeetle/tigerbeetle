#!/usr/bin/env bash
set -eu -o pipefail

PACKAGE_JSON_VERSION=$(jq -r '.version' package.json)
NPM_SHOW_OUTPUT=$(npm show "tigerbeetle-node@${PACKAGE_JSON_VERSION}" --json 2>/dev/null || echo -n "")
NPM_VERSION_EXISTS=$(test -z "${NPM_SHOW_OUTPUT}" && echo "false" || echo "true")

NPM_INTEGRITY=$(echo "${NPM_SHOW_OUTPUT}" | jq -r '.dist.integrity')

CURRENT_INTEGRITY=$(npm --json pack --pack-destination /tmp/ | grep -v -E '^>' | jq -r '.[0].integrity')

echo "Version in package.json: ${PACKAGE_JSON_VERSION}"
echo "Does it exist on NPM?    ${NPM_VERSION_EXISTS}"
echo "Integrity of current build: ${CURRENT_INTEGRITY}"
echo "Integrity of NPM version:   ${NPM_INTEGRITY}"
echo

if [ "${NPM_VERSION_EXISTS}" != "false" ]; then
    echo "package.json version matches NPM version"
    if [ "${NPM_INTEGRITY}" != "${CURRENT_INTEGRITY}" ]; then
        echo "NPM integrity differs from current integrity for the same version number. You'll need to increment the version under package.json to cut a new release. Bailing out."
        exit 1
    else
        echo "NPM integrity and current integrity match."
    fi
else
    echo "package.json version differs from NPM version - a new release will be created"
fi
