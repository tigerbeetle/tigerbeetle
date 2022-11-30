#!/usr/bin/env bash
set -eu -o pipefail
shopt -s dotglob

PACKAGE_JSON_VERSION=$(jq -r '.version' package.json)
NPM_SHOW_OUTPUT=$(npm show "tigerbeetle-node@${PACKAGE_JSON_VERSION}" --json 2>/dev/null || echo -n "")
NPM_VERSION_EXISTS=$(test -z "${NPM_SHOW_OUTPUT}" && echo "false" || echo "true")
TMP_DIR=$(mktemp -d)

function cleanup {
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

echo "Version in package.json: ${PACKAGE_JSON_VERSION}"
echo "Does it exist on NPM?    ${NPM_VERSION_EXISTS}"

if [ "${NPM_VERSION_EXISTS}" != "false" ]; then
    mkdir "${TMP_DIR}/current"

    NPM_TARBALL=$(echo "${NPM_SHOW_OUTPUT}" | jq -r '.dist.tarball')
    curl "${NPM_TARBALL}" 2>/dev/null | tar -xzf - -C "${TMP_DIR}"

    npm --json pack --pack-destination "${TMP_DIR}/current" &> /dev/null
    tar -xzf "${TMP_DIR}/current/tigerbeetle-node"*.tgz -C "${TMP_DIR}/current"

    # We pull in src/io/darwin.zig and src/io/windows.zig explicitly here: the hash of our binary will only
    # include src/io/linux.zig due to a comptime switch.
    NPM_DIST_INTEGRITY=$(cat "${TMP_DIR}/package/dist/"* "${TMP_DIR}/package/src/tigerbeetle/src/io/"{darwin.zig,windows.zig} | sha256sum | awk '{print $1}')
    CURRENT_DIST_INTEGRITY=$(cat "${TMP_DIR}/current/package/dist/"* "${TMP_DIR}/current/package/src/tigerbeetle/src/io/"{darwin.zig,windows.zig} | sha256sum | awk '{print $1}')

    echo "Integrity of current build: ${CURRENT_DIST_INTEGRITY}"
    echo "Integrity of NPM version:   ${NPM_DIST_INTEGRITY}"

    echo
    echo "package.json version matches NPM version"

    if [ "${NPM_DIST_INTEGRITY}" != "${CURRENT_DIST_INTEGRITY}" ]; then
        echo "NPM integrity differs from current integrity for the same version number. You'll need to increment the version under package.json to cut a new release. Bailing out."
        exit 1
    else
        echo "NPM integrity and current integrity match."
    fi
else
    echo
    echo "package.json version differs from NPM version - a new release will be created"
fi
