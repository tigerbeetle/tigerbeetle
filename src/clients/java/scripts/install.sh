#!/bin/sh
set -eu

git submodule init
git submodule update

echo "Installing TigerBeetle..."
(cd ../../.. && ./scripts/install.sh)

echo "Building TigerBeetle Java Client..."
mvn -e -B package -Dmaven.test.skip -Djacoco.skip --quiet
