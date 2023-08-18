#!/bin/sh

set -eu

docker run -v "$(pwd)":/build:ro -w /tmp node:19 bash -c '
set -eux

# Deals with an error git throws within Docker when a git repo is
# volume mounted.:
#   fatal: detected dubious ownership in repository at '/wrk'
git config --global --add safe.directory /build

# Using a fork of this validate-links check to include checks on
# absolute links until
# https://github.com/remarkjs/remark-validate-links/issues/75 is fixed
# upstream.
npm install cspell@^6.31.1 remark-cli@^11.0.0 https://github.com/tigerbeetle/remark-validate-links

# Validate links
npx remark --use remark-validate-links --frail /build

cd /build

# Validate dictionary is formatted correctly
curl -L -o /tmp/jq https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
chmod +x /tmp/jq
cat scripts/.cspell.json | /tmp/jq empty

# Spellcheck
files="$(git ls-files -s | grep -v ^16 | cut -f2- | grep \.md)"
npx cspell --config ./scripts/.cspell.json $files
'
