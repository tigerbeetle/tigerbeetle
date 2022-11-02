#!/usr/bin/env bash

set -e

# This script builds the docs website for the currently checked out
# branch.

git clone https://github.com/tigerbeetledb/docs docs_website
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
( cd docs_website && npm install && ./scripts/build.sh "$BRANCH" )
rm -rf docs_website
