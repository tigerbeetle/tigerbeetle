#!/usr/bin/env bash

# TODO(owickstrom) port to build.zig

set -eu

usage() {
  echo "usage: ${0##*/} <tag>"
  echo "Push the TigerBeetle Docker images to Antithesis' registry."
}

if [ $# -ne 1 ] || [ "$1" = '-h' ]; then
  usage >&2
  exit 1
fi
tag=$1
url_prefix='us-central1-docker.pkg.dev/molten-verve-216720/tigerbeetle-repository'

push_image() {
  image=$1
  url="$url_prefix/$image:$tag"

  docker tag "$image:$tag" "$url"
  docker push              "$url"
}

push_image config
push_image replica
push_image workload
