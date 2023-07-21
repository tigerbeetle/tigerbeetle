#!/usr/bin/env sh
set -eu

ZIG_RELEASE_DEFAULT="0.10.1"
# Default to the release build, or allow the latest dev build, or an explicit release version:
ZIG_RELEASE=${1:-$ZIG_RELEASE_DEFAULT}
if [ "$ZIG_RELEASE" = "latest" ]; then
    ZIG_RELEASE="builds"
fi

# Validate the release version explicitly:
if echo "$ZIG_RELEASE" | grep -q '^builds$'; then
    echo "Installing Zig latest build..."
elif echo "$ZIG_RELEASE" | grep -q '^[0-9]\+.[0-9]\+.[0-9]\+$'; then
    echo "Installing Zig $ZIG_RELEASE release build..."
else
    echo "Release version invalid"
    exit 1
fi

# Determine the architecture:
if [ "$(uname -m)" = 'arm64' ] || [ "$(uname -m)" = 'aarch64' ]; then
    ZIG_ARCH="aarch64"
else
    ZIG_ARCH="x86_64"
fi

# Determine the operating system:
if [ "$(uname)" = "Linux" ]; then
    ZIG_OS="linux"
else
    ZIG_OS="macos"
fi

ZIG_TARGET="zig-$ZIG_OS-$ZIG_ARCH"

# Determine the build, split the JSON line on whitespace and extract the 2nd field, then remove quotes and commas:
if command -v wget; then
    # -4 forces `wget` to connect to ipv4 addresses, as ipv6 fails to resolve on certain distros.
    # Only A records (for ipv4) are used in DNS:
    ipv4="-4"
    # But Alpine doesn't support this argument
    if [ -f /etc/alpine-release ]; then
	ipv4=""
    fi
    # shellcheck disable=SC2086 # We control ipv4 and it'll always either be empty or -4
    ZIG_URL=$(wget $ipv4 --quiet -O - https://ziglang.org/download/index.json | grep -F "$ZIG_TARGET" | grep -F "$ZIG_RELEASE" | awk '{print $2}' | sed 's/[",]//g')
else
    ZIG_URL=$(curl --silent https://ziglang.org/download/index.json | grep -F "$ZIG_TARGET" | grep -F "$ZIG_RELEASE" | awk '{print $2}' | sed 's/[",]//g')
fi

# Ensure that the release is actually hosted on the ziglang.org website:
if [ -z "$ZIG_URL" ]; then
    echo "Release not found on ziglang.org"
    exit 1
fi

# Work out the filename from the URL, as well as the directory without the ".tar.xz" file extension:
ZIG_TARBALL=$(basename "$ZIG_URL")
ZIG_DIRECTORY=$(basename "$ZIG_TARBALL" .tar.xz)

# Download, making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
echo "Downloading $ZIG_URL..."
if command -v wget; then
    # -4 forces `wget` to connect to ipv4 addresses, as ipv6 fails to resolve on certain distros.
    # Only A records (for ipv4) are used in DNS:
    ipv4="-4"
    # But Alpine doesn't support this argument
    if [ -f /etc/alpine-release ]; then
	ipv4=""
    fi
    # shellcheck disable=SC2086 # We control ipv4 and it'll always either be empty or -4
    wget $ipv4 --quiet --output-document="$ZIG_TARBALL" "$ZIG_URL"
else
    curl --silent --output "$ZIG_TARBALL" "$ZIG_URL"
fi

# Extract and then remove the downloaded tarball:
echo "Extracting $ZIG_TARBALL..."
tar -xf "$ZIG_TARBALL"
rm "$ZIG_TARBALL"

# Replace any existing Zig installation so that we can install or upgrade:
echo "Installing $ZIG_DIRECTORY to 'zig' in current working directory..."
rm -rf zig
mv "$ZIG_DIRECTORY" zig

# It's up to the user to add this to their path if they want to:
ZIG_BIN="$(pwd)/zig/zig"

ZIG_VERSION=$($ZIG_BIN version)
echo "Congratulations, you have successfully installed Zig $ZIG_VERSION to $ZIG_BIN. Enjoy!"
