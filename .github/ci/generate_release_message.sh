#!/usr/bin/env bash

VERSION=$1
if [[ "$VERSION" == "" ]]; then
    echo "Missing version / arg1. Usage: \`./scripts/generate_release_message.sh 0.13.1\`."
    exit 1
fi

set -eu

# https://hub.github.com/hub-release.1.html
# > The text up to the first blank line in MESSAGE is treated as the
# > release title, and the rest is used as release description in
# > Markdown format.

message="$VERSION

**Automated build. Do not use in production.**

**NOTE**: You must run the same version of server and client. We do
not yet follow semantic versioning where all patch releases are
interchangeable.

## Server

* Binary: Download the zip for your OS and architecture from this page and unzip.
* Docker: \`docker pull ghcr.io/tigerbeetledb/tigerbeetle:$VERSION\`
* Docker (debug image): \`docker pull ghcr.io/tigerbeetledb/tigerbeetle:$VERSION-debug\`

## Clients

**NOTE**: Because of package manager caching, it may take a few
  minutes after the release for this version to appear in the package
  manager.

* .NET: \`dotnet add package tigerbeetle --version $VERSION\`
* Go: \`go mod edit -require github.com/tigerbeetledb/tigerbeetle-go@v${VERSION}\`
* Java: Update the version of \`com.tigerbeetle.tigerbeetle-java\` in \`pom.xml\` to \`$VERSION\`.
* Node.js: \`npm install tigerbeetle-node@$VERSION\`
"

echo "$message"
