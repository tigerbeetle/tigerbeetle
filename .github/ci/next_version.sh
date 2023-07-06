#!/usr/bin/env sh

set -eu

# Update these as needed.
MAJOR=0
MINOR=13

git fetch --tags --all
version="$MAJOR.$MINOR.0"
patch="$(git tag | grep $MAJOR.$MINOR | cut -d '.' -f 3 | sort -nr | head -n1)"
if [ "$patch" != "" ]; then
    version="$MAJOR.$MINOR.$((patch+1))"
fi
echo "$version"
