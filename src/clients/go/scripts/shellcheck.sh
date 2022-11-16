#!/usr/bin/env bash

set -e

docker run -v "$(pwd)":/wrk -w /wrk koalaman/shellcheck scripts/*.sh
