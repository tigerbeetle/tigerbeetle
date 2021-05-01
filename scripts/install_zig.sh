#!/bin/bash
set -e

# Default to master while we wait for the 0.8.0 release:
# TODO When 0.8.0 arrives, we should make the release build the default.
if [ "$1" == "tagged" ]; then
    ZIG_RELEASE="0.7.1"
    echo "Installing $ZIG_RELEASE release build..."
else
    ZIG_RELEASE="builds"
    echo "Installing latest master build..."
fi

# Determine the platform:
if [[ "$OSTYPE" == "darwin"* ]]; then
    ZIG_TARGET="zig-macos-x86_64"
else
    ZIG_TARGET="zig-linux-x86"
fi

# Determine the 0.7.1 or latest master build, split the JSON line on whitespace and extract the 2nd field, then remove quotes and commas:
if command -v wget &> /dev/null; then
    ZIG_URL=`wget --quiet -O - https://ziglang.org/download/index.json | grep "$ZIG_TARGET" | grep "$ZIG_RELEASE" | awk '{print $2}' | sed 's/[",]//g'`
else
    ZIG_URL=`curl --silent https://ziglang.org/download/index.json | grep "$ZIG_TARGET" | grep "$ZIG_RELEASE" | awk '{print $2}' | sed 's/[",]//g'`
fi

# Work out the filename from the URL, as well as the directory without the ".tar.xz" file extension:
ZIG_TARBALL=`basename "$ZIG_URL"`
ZIG_DIRECTORY=`basename "$ZIG_TARBALL" .tar.xz`

# Download, making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
echo "Downloading $ZIG_URL..."
if command -v wget &> /dev/null; then
    wget --quiet --show-progress --output-document=$ZIG_TARBALL $ZIG_URL
else
    curl --silent --progress-bar --output $ZIG_TARBALL $ZIG_URL
fi

# Extract and then remove the downloaded tarball:
echo "Extracting $ZIG_TARBALL..."
tar -xf $ZIG_TARBALL
rm $ZIG_TARBALL

# Replace any existing Zig installation so that we can install or upgrade:
echo "Installing $ZIG_DIRECTORY to 'zig' in current working directory..."
rm -rf zig
mv $ZIG_DIRECTORY zig

# It's up to the user to add this to their path if they want to:
ZIG_BIN="$(pwd)/zig/zig"

ZIG_VERSION=`$ZIG_BIN version`
echo "Congratulations, you have successfully installed Zig $ZIG_VERSION to $ZIG_BIN. Enjoy!"
