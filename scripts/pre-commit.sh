#!/usr/bin/env bash

# Usage:
# ln -s ../../scripts/pre-commit.sh .git/hooks/pre-commit

set -euo pipefail
cd "$(dirname "$0")/../.."

zig fmt --check .