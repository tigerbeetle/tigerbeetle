#!/bin/bash
set -e

ZIG_RELEASE="0.7.1"

if [ "$1" == "master" ]; then
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
echo "Installing $ZIG_DIRECTORY to /usr/local/lib/zig (requires root)..."
sudo rm -rf /usr/local/lib/zig
sudo mv $ZIG_DIRECTORY /usr/local/lib/zig

# Symlink the Zig binary into /usr/local/bin, which should already be in the user's path:
sudo ln -s -f /usr/local/lib/zig/zig /usr/local/bin/zig

ZIG_VERSION=`zig version`
echo "Congratulations, you have successfully installed Zig $ZIG_VERSION. Enjoy!"
