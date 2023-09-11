#!/usr/bin/env sh

set -eu

# Update these as needed.
MAJOR=0
MINOR=13

# Our Docker tags and GH releases can be overwritten, but if we've had a partial release
# due to a failure for dotnet, npm or Java, we need to take that version into account too.
NUGET_INDEX="https://api.nuget.org/v3-flatcontainer/tigerbeetle/index.json"
MAVEN_INDEX="https://repo1.maven.org/maven2/com/tigerbeetle/tigerbeetle-java/maven-metadata.xml"

NUGET_VERSION=$(curl -s "${NUGET_INDEX}" | jq -r .versions[-1])
MAVEN_VERSION=$(curl -s "${MAVEN_INDEX}" | xq -r .metadata.versioning.latest)
NPM_VERSION=$(npm view tigerbeetle-node version)

nuget_patch=$(echo "${NUGET_VERSION}" | grep "$MAJOR\.$MINOR" | cut -d '.' -f 3)
maven_patch=$(echo "${NUGET_VERSION}" | grep "$MAJOR\.$MINOR" | cut -d '.' -f 3)
npm_patch=$(echo "${NUGET_VERSION}" | grep "$MAJOR\.$MINOR" | cut -d '.' -f 3)

git fetch --tags --all
version="$MAJOR.$MINOR.0"
git_patch="$(git tag | grep "$MAJOR\.$MINOR" | cut -d '.' -f 3 | sort -nr | head -n1)"

echo "Current Nuget version: ${NUGET_VERSION}" 1>&2
echo "Current Maven version: ${MAVEN_VERSION}" 1>&2
echo "Current NPM version: ${NPM_VERSION}" 1>&2
echo "Current git version: ${MAJOR}.${MINOR}.${git_patch}" 1>&2

latest_patch=$(printf "%s\n" "${nuget_patch}" "${maven_patch}" "${npm_patch}" "${git_patch}" | sort -rn | head -1)

if [ "$git_patch" != "" ]; then
    version="$MAJOR.$MINOR.$((latest_patch+1))"
fi
echo "$version"
