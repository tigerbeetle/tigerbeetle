#!/bin/sh -eu

usage() {
	cat <<-EOF
	usage: ${0##*/} <tag>

	Build TigerBeetle Docker images for Antithesis, with the specified tag.
	EOF
}

if [ $# -ne 1 ] || [ "$1" = '-h' ]; then
	usage >&2
	exit 1
fi
tag=$1

# This tag is used in `config/docker-compose.yaml` to select the images to run.
echo "TAG=$tag" > ./tools/antithesis/config/.env

# XXX sudo
sudo docker build --file=./tools/antithesis/api.Dockerfile           --tag="api:$tag"  .
sudo docker build --file=./tools/antithesis/configuration.Dockerfile --tag="config:$tag"  .
sudo docker build --file=./tools/antithesis/replica.Dockerfile       --tag="replica:$tag" .
sudo docker build --file=./tools/antithesis/workload.Dockerfile      --tag="workload:$tag"  .
