#!/usr/bin/env bash

set -xe
REPO=git@github.com:matklad/tigerbeetle-filter-repo.git

git clone $REPO original
git clone $REPO filtered

pushd filtered
git filter-repo --invert-paths \
  --path transfers \
  --path docs-site/package-lock.json \
  --path-glob */assets/js/main.*.js \
  --path-glob '*.a' \
  --path-glob '*.cjs' \
  --path-glob '*.dll' \
  --path-glob '*.dylib' \
  --path-glob '*.lib' \
  --path-regex '^(.*)/(?!favicon)([^/]*)\.png$' \
  --path-glob '*.so'
popd

diff --recursive --exclude .git original filtered

pushd filtered
git remote add origin $REPO
git push origin --force --all
git push origin --force --tags
popd

echo "ok."
