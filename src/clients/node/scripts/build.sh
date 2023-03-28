#!/usr/bin/env bash
# This script builds our release node package. It pulls in
# node:14-buster specifically, to be on an old-ish version of glibc
# with the minimum node version we support for building.
#
# Both glibc and NAPI have backward-compatible ABIs
set -e

docker run -v "$(pwd)/../../..":/wrk -w /wrk --entrypoint bash node:14-buster -c '
wget --output-document="/tmp/node-headers.tar.gz" "$(node -p 'process.release.headersUrl')"
tar -xf "/tmp/node-headers.tar.gz" --strip-components=1 -C /usr/local
./scripts/install_zig.sh

cd src/clients/node
npm run clean
npm config set cache /tmp --global
npm install --unsafe-perm --no-save

rm -f tigerbeetle-node-*.tgz
npm pack --unsafe-perm
'
