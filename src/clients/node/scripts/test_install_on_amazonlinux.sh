#!/usr/bin/env bash

set -e

./scripts/build.sh

# Prebuild the container with this directory because we have no need for its artifacts
id=$(docker build -q -f - . < <(echo "
FROM amazonlinux
COPY . /wrk"))

docker run -w /test "$id" sh -c "
set -e
yum update -y
yum install -y xz wget git glibc tar
wget -O- -q https://rpm.nodesource.com/setup_16.x | bash -
yum install -y nodejs
npm install /wrk
node -e 'require(\"tigerbeetle-node\"); console.log(\"SUCCESS!\")'
"
