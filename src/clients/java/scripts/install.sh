#!/bin/bash
set -eEuo pipefail

git submodule init
git submodule update

echo "Installing TigerBeetle..."
(cd ../../.. && ./scripts/install.sh)

echo "Building TigerBeetle Java Client..."
(cd ./src/tigerbeetle-java && mvn -B package --quiet)