#!/usr/bin/env sh

set -eu

# Update these as needed.
MAJOR=0
MINOR=13

git fetch --tags --all
version="$MAJOR.$MINOR.0"
latest="$(git tag | grep $MAJOR.$MINOR | sort -r | head -n1)"
if [ "$latest" != "" ]; then
    patch="$(echo "$latest" | cut -d '.' -f 3)"
    version="$MAJOR.$MINOR.$((patch+1))"
fi
echo "$version"
