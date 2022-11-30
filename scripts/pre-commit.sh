#!/usr/bin/env bash

# Usage:
# ln -s ../../scripts/pre-commit.sh .git/hooks/pre-commit

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

zig fmt --check .
