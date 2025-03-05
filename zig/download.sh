#!/usr/bin/env sh
set -eu

ZIG_RELEASE_DEFAULT="0.14.0"
# Default to the release build, or allow the latest dev build, or an explicit release version:
ZIG_RELEASE=${1:-$ZIG_RELEASE_DEFAULT}
if [ "$ZIG_RELEASE" = "latest" ]; then
    ZIG_RELEASE="builds"
fi

# Validate the release version explicitly:
if echo "$ZIG_RELEASE" | grep -q '^builds$'; then
    echo "Downloading Zig latest build..."
elif echo "$ZIG_RELEASE" | grep -q '^[0-9]\+.[0-9]\+.[0-9]\+$'; then
    echo "Downloading Zig $ZIG_RELEASE release build..."
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
case "$(uname)" in
    Linux)
        ZIG_OS="linux"
        ;;
    Darwin)
        ZIG_OS="macos"
        ;;
    CYGWIN*)
        ZIG_OS="windows"
        ;;
    *)
        echo "Unknown OS"
        exit 1
        ;;
esac

ZIG_TARGET="zig-$ZIG_OS-$ZIG_ARCH"

# Determine the build, split the JSON line on whitespace and extract the 2nd field, then remove quotes and commas:
if command -v wget > /dev/null; then
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
ZIG_ARCHIVE=$(basename "$ZIG_URL")

case "$ZIG_ARCHIVE" in
    *".tar.xz")
        ZIG_ARCHIVE_EXT=".tar.xz"
        ;;
    *".zip")
        ZIG_ARCHIVE_EXT=".zip"
        ;;
    *)
        echo "Unknown archive extension"
        exit 1
        ;;
esac

ZIG_DIRECTORY=$(basename "$ZIG_ARCHIVE" "$ZIG_ARCHIVE_EXT")

# Download, making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
echo "Downloading $ZIG_URL..."
if command -v wget > /dev/null; then
    # -4 forces `wget` to connect to ipv4 addresses, as ipv6 fails to resolve on certain distros.
    # Only A records (for ipv4) are used in DNS:
    ipv4="-4"
    # But Alpine doesn't support this argument
    if [ -f /etc/alpine-release ]; then
    ipv4=""
    fi
    # shellcheck disable=SC2086 # We control ipv4 and it'll always either be empty or -4
    wget $ipv4 --quiet --output-document="$ZIG_ARCHIVE" "$ZIG_URL"
else
    curl --silent --output "$ZIG_ARCHIVE" "$ZIG_URL"
fi

# Extract and then remove the downloaded archive:
echo "Extracting $ZIG_ARCHIVE..."
case "$ZIG_ARCHIVE_EXT" in
    ".tar.xz")
        tar -xf "$ZIG_ARCHIVE"
        ;;
    ".zip")
        unzip -q "$ZIG_ARCHIVE"
        ;;
    *)
        echo "Unexpected error"
        exit 1
        ;;
esac
rm "$ZIG_ARCHIVE"

# Replace these existing directories and files so that we can install or upgrade:
rm -rf zig/doc
rm -rf zig/lib
mv "$ZIG_DIRECTORY/LICENSE" zig/
mv "$ZIG_DIRECTORY/README.md" zig/
mv "$ZIG_DIRECTORY/doc" zig/
mv "$ZIG_DIRECTORY/lib" zig/
mv "$ZIG_DIRECTORY/zig" zig/

# We expect to have now moved all directories and files out of the extracted directory.
# Do not force remove so that we can get an error if the above list of files ever changes:
rmdir "$ZIG_DIRECTORY"

# It's up to the user to add this to their path if they want to:
ZIG_BIN="$(pwd)/zig/zig"
echo "Downloading completed ($ZIG_BIN)! Enjoy!"
