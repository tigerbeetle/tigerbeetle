#!/bin/bash
set -e

ZIG_RELEASE="0.7.1"

if [ "$1" == "master" ]; then
    ZIG_RELEASE="builds"
    echo "Installing latest master build..."
fi

# Determine the 0.7.1 or latest Linux master build, split the JSON line on whitespace and extract the 2nd field, then remove quotes and commas:
ZIG_URL=`wget --quiet -O - https://ziglang.org/download/index.json | grep zig-linux-x86 | grep "$ZIG_RELEASE" | awk '{print $2}' | sed 's/[",]//g'`

# Work out the filename from the URL, as well as the directory without the ".tar.xz" file extension:
ZIG_TARBALL=`basename "$ZIG_URL"`
ZIG_DIRECTORY=`basename "$ZIG_TARBALL" .tar.xz`

# Download, making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
echo "Downloading $ZIG_URL..."
wget --quiet --show-progress --output-document=$ZIG_TARBALL $ZIG_URL

# Extract and then remove the downloaded tarball:
echo "Extracting $ZIG_TARBALL..."
tar -xf $ZIG_TARBALL
rm $ZIG_TARBALL

# Replace any existing Zig installation so that we can install or upgrade:
echo "Installing $ZIG_DIRECTORY to /usr/local/lib/zig (requires root)..."
sudo rm -rf /usr/local/lib/zig
sudo mv $ZIG_DIRECTORY /usr/local/lib/zig

# Symlink the Zig binary into /usr/local/bin, which should already be in the user's path:
sudo ln -s --force /usr/local/lib/zig/zig /usr/local/bin/zig

ZIG_VERSION=`zig version`
echo "Congratulations, you have successfully installed Zig $ZIG_VERSION. Enjoy!"
