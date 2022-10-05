#!/usr/bin/env bash

set -e

if [[ -z "$1" ]]; then
    echo "Expected a first argument with a Docker image name or id"
    exit 2
fi

if [[ "$2" != "--want-production" ]] && [[ "$2" != "--want-debug" ]]; then
    echo "Expected a second argument: --want-production or --want-debug".
    exit 2
fi

check="
set -eu

apt-get update -y
apt-get install -y binutils

is_production=false
# This getSymbolFromDwarf symbol only appears to be in the debug
# build, not the release build. So that's the test!
if ! [[ \$(nm -an /opt/beta-beetle/tigerbeetle | grep getSymbolFromDwarf) ]]; then
  is_production=true
fi
"

if [[ "$2" == '--want-production' ]]; then
    cmd+="
if [[ \$is_production == false ]]; then
  echo 'Does not seem to be a production build'
  exit 1
fi"
else
    cmd+="
if [[ \$is_production == true ]]; then
  echo 'Does not seem to be a debug build'
  exit 1
fi"
fi

# We accept a passed-in arg 
docker run --entrypoint bash "$1" -c "$check"
