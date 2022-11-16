#!/usr/bin/env bash

set -e

./scripts/build.sh

id=$(docker build -q -f - . < <(echo "
FROM fedora
COPY . /wrk"))
docker run -w /test "$id" sh -c "
set -e
dnf update -y
dnf install -y xz wget git
wget -O- -q https://rpm.nodesource.com/setup_18.x | bash -
dnf install -y nodejs
npm install /wrk
node -e 'require(\"tigerbeetle-node\"); console.log(\"SUCCESS!\")'
"
