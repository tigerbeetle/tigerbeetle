#!/bin/sh

set -eu

docker run -v "$(pwd)":/build:ro -w /tmp node:19 bash -c '
set -eux

# Deals with an error git throws within Docker when a git repo is
# volume mounted.:
#   fatal: detected dubious ownership in repository at '/wrk'
git config --global --add safe.directory /build

npm install cspell@^6.31.1

cd /build

# Validate dictionary is formatted correctly
curl -L -o /tmp/jq https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
chmod +x /tmp/jq
cat docs/.cspell.json | /tmp/jq empty

# Spellcheck
files="$(git ls-files -s | grep -v ^16 | cut -f2- | grep \\.md)"
npx cspell --config ./docs/.cspell.json $files
'
