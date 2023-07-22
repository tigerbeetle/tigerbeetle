#!/usr/bin/env bash

set -e

git config --global user.email "bot@tigerbeetle.com"
git config --global user.name "TigerBeetle Bot"

mkdir -p ~/.ssh
echo -e "${TIGERBEETLE_DOCS_DEPLOY_KEY}" > ~/.ssh/id_ed25519
chmod 600 ~/.ssh/id_ed25519

git clone git@github.com:tigerbeetle/docs.git ~/docs

cd ~/docs

./zig/zig build "${TIGERBEETLE_COMMIT}"
git add -A
git commit -m "Update docs for ${TIGERBEETLE_COMMIT}"
git push origin main
