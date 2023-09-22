#!/usr/bin/env bash

set -e

if [ -z ${SKIP_NODE_BUILD+x} ]; then
	./scripts/build.sh
fi

# Prebuild the container with this directory because we have no need for its artifacts

# Can unpin once https://github.com/nodesource/distributions/issues/1524 is fixed
id=$(docker build -q -f - ../../.. < <(echo "
FROM amazonlinux:2.0.20230307.0
COPY . /wrk"))

docker run -w /test "$id" sh -c "
set -e
yum update -y
yum install -y xz wget git glibc tar
yum install https://rpm.nodesource.com/pub_16.x/nodistro/repo/nodesource-release-nodistro-1.noarch.rpm -y
yum install nodejs -y --setopt=nodesource-nodejs.module_hotfixes=1
npm install /wrk/src/clients/node/tigerbeetle-node-*.tgz
node -e 'require(\"tigerbeetle-node\"); console.log(\"SUCCESS!\")'
"
