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

# Extract the TigerBeetle binary from the passed in Docker image
scratch=$(mktemp)
function finish {
  rm -f "${scratch}"
}
trap finish EXIT

id=$(docker create "${1}")
docker cp "${id}:/tigerbeetle" "${scratch}"
docker rm "${id}"

is_production="false"
# This getSymbolFromDwarf symbol only appears to be in the debug
# build, not the release build. So that's the test!
if ! (nm -an "${scratch}" | grep -q getSymbolFromDwarf); then
  is_production="true"
fi

echo "Is production: ${is_production}"

if [[ "$2" == '--want-production' ]]; then
    if [[ "${is_production}" == "false" ]]; then
      echo 'Does not seem to be a production build'
      exit 1
    fi
else
    if [[ "${is_production}" == "true" ]]; then
      echo 'Does not seem to be a debug build'
      exit 1
    fi
fi
