#!/bin/bash
set -e

ZIG_RELEASE_DEFAULT="0.9.1"

# Default to the release build, or allow the latest dev build, or an explicit release version:
if [ -z "$1" ]; then
    ZIG_RELEASE=$ZIG_RELEASE_DEFAULT
elif [ "$1" == "latest" ]; then
    ZIG_RELEASE="builds"
else
    ZIG_RELEASE=$1
fi

# Validate the release version explicitly:
if [[ $ZIG_RELEASE =~ ^builds$ ]]; then
    echo "Installing Zig latest build..."
elif [[ $ZIG_RELEASE =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Installing Zig $ZIG_RELEASE release build..."
else
    echo "Release version invalid"
    exit 1
fi

# Determine the architecture:
if [ `uname -m` == 'arm64' ] || [ `uname -m` == 'aarch64' ]; then
    ZIG_ARCH="aarch64"
else
    ZIG_ARCH="x86_64"
fi

# Determine the operating system:
if [[ "$OSTYPE" == "darwin"* ]]; then
    ZIG_OS="macos"
else
    ZIG_OS="linux"
fi

ZIG_TARGET="zig-$ZIG_OS-$ZIG_ARCH"

# Determine the build, split the JSON line on whitespace and extract the 2nd field, then remove quotes and commas:
if command -v wget &> /dev/null; then
    # -4 forces `wget` to connect to ipv4 addresses, as ipv6 fails to resolve on certain distros.
    # Only A records (for ipv4) are used in DNS:
    ZIG_URL=`wget -4 --quiet -O - https://ziglang.org/download/index.json | grep -F "$ZIG_TARGET" | grep -F "$ZIG_RELEASE" | awk '{print $2}' | sed 's/[",]//g'`
else
    ZIG_URL=`curl --silent https://ziglang.org/download/index.json | grep -F "$ZIG_TARGET" | grep -F "$ZIG_RELEASE" | awk '{print $2}' | sed 's/[",]//g'`
fi

# Ensure that the release is actually hosted on the ziglang.org website:
if [ -z "$ZIG_URL" ]; then
    echo "Release not found on ziglang.org"
    exit 1
fi

# Work out the filename from the URL, as well as the directory without the ".tar.xz" file extension:
ZIG_TARBALL=`basename "$ZIG_URL"`
ZIG_DIRECTORY=`basename "$ZIG_TARBALL" .tar.xz`

# Download, making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
echo "Downloading $ZIG_URL..."
if command -v wget &> /dev/null; then
    # -4 forces `wget` to connect to ipv4 addresses, as ipv6 fails to resolve on certain distros.
    # Only A records (for ipv4) are used in DNS:
    wget -4 --quiet --show-progress --output-document=$ZIG_TARBALL $ZIG_URL
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
