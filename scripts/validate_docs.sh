#!/usr/bin/env bash

set -e

# This script builds the docs website for the currently checked out
# branch.

git clone https://github.com/tigerbeetledb/docs docs_website
# Try to grab branch from Github Actions CI.
# See also: https://docs.github.com/en/actions/learn-github-actions/environment-variables.
env
BRANCH="$GITHUB_REF_NAME"
if [[ -z "$BRANCH" ]]; then
    # Otherwise fall back to git rev-parse
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
fi
( cd docs_website && npm install && ./scripts/build.sh "$BRANCH" )
rm -rf docs_website
