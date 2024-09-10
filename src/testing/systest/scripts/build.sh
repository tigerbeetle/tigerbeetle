#!/usr/bin/env bash

set -eu

# TODO(owickstrom) port to build.zig

usage() {
  echo "usage: ${0##*/} <tag>"
  echo "Build TigerBeetle Docker images for system test, with the specified tag."
}

if [ $# -ne 1 ] || [ "$1" = '-h' ]; then
  usage >&2
  exit 1
fi

tag="$1"

zig build -Drelease
(cd src/clients/java && mvn clean install -Dmaven.test.skip)
(cd src/testing/systest/workload && mvn clean package)

docker build --file=./src/testing/systest/configuration.Dockerfile --build-arg "TAG=${tag}" --tag="config:${tag}" .
docker build --file=./src/testing/systest/replica.Dockerfile       --tag="replica:${tag}" .
docker build --file=./src/testing/systest/workload.Dockerfile      --tag="workload:${tag}" .
