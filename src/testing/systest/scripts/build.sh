#!/bin/sh -eu

# TODO(owickstrom) port to build.zig

usage() {
	cat <<-EOF
	usage: ${0##*/} <tag>

	Build TigerBeetle Docker images for system test, with the specified tag.
	EOF
}

if [ $# -ne 1 ] || [ "$1" = '-h' ]; then
	usage >&2
	exit 1
fi
tag=$1


(cd src/clients/java && mvn package -Dmaven.test.skip)
(cd src/testing/systest/workload && mvn package)


docker build --file=./src/testing/systest/configuration.Dockerfile --build-arg "TAG=$tag" --tag="config:$tag" .
docker build --file=./src/testing/systest/replica.Dockerfile       --tag="replica:$tag" .
docker build --file=./src/testing/systest/workload.Dockerfile      --tag="workload:$tag" .
