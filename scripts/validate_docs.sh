#!/usr/bin/env bash

set -e

# This script builds the docs website for the currently checked out
# branch.

git clone https://github.com/tigerbeetledb/docs docs_website
# Try to grab branch from Github Actions CI.
# See also: https://docs.github.com/en/actions/learn-github-actions/environment-variables.
BRANCH="$GITHUB_HEAD_REF"
if [[ -z "$BRANCH" ]]; then
    # Otherwise fall back to git rev-parse
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
fi

REPO="https://github.com/tigerbeetledb/tigerbeetle"
if ! [[ -z "GITHUB_REPOSITORY" ]]; then
    REPO="https://github.com/${GITHUB_REPOSITORY}"
fi

( cd docs_website && npm install && ./scripts/build.sh "$BRANCH" "$REPO" )
rm -rf docs_website
