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

# This tag is used in `config/docker-compose.yaml` to select the images to run.
echo "TAG=$tag" > ./tools/system_test/config/.env

docker build --file=./tools/system_test/configuration.Dockerfile --tag="config:$tag"  .
docker build --file=./tools/system_test/replica.Dockerfile       --tag="replica:$tag" .
docker build --file=./tools/system_test/workload.Dockerfile      --tag="workload:$tag"  .
