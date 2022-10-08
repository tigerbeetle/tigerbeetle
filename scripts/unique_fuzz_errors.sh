#!/usr/bin/env bash
set -eEuo pipefail

# After running ./fuzz_repeatedly.sh use this script to produce a list of unique crashes.
# As a heuristic, we look for the first line of the stacktrace that occurs inside tigerbeetle code.

grep -m 1 'tigerbeetle/src' fuzz_* | sort -u -t':' -k2,2