#!/usr/bin/env bash

set -eu

if [[ "$(git diff)" != "" ]]; then
    printf "\033[0;31mFAILURE: Unexpected diff (did you run 'yarn format'?)\n\n\033[0m"
    git diff --color=never
    exit 1
fi
