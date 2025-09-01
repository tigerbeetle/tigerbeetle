#!/usr/bin/env sh
set -eu

ZIG_MIRROR="https://pkg.machengine.org/zig"
ZIG_RELEASE="0.14.1"
ZIG_CHECKSUMS=$(cat<<EOF
${ZIG_MIRROR}/0.14.1/zig-aarch64-linux-0.14.1.tar.xz f7a654acc967864f7a050ddacfaa778c7504a0eca8d2b678839c21eea47c992b
${ZIG_MIRROR}/0.14.1/zig-aarch64-macos-0.14.1.tar.xz 39f3dc5e79c22088ce878edc821dedb4ca5a1cd9f5ef915e9b3cc3053e8faefa
${ZIG_MIRROR}/0.14.1/zig-aarch64-windows-0.14.1.zip b5aac0ccc40dd91e8311b1f257717d8e3903b5fefb8f659de6d65a840ad1d0e7
${ZIG_MIRROR}/0.14.1/zig-x86_64-linux-0.14.1.tar.xz 24aeeec8af16c381934a6cd7d95c807a8cb2cf7df9fa40d359aa884195c4716c
${ZIG_MIRROR}/0.14.1/zig-x86_64-macos-0.14.1.tar.xz b0f8bdfb9035783db58dd6c19d7dea89892acc3814421853e5752fe4573e5f43
${ZIG_MIRROR}/0.14.1/zig-x86_64-windows-0.14.1.zip 554f5378228923ffd558eac35e21af020c73789d87afeabf4bfd16f2e6feed2c
EOF
)

echo "Downloading Zig $ZIG_RELEASE release build..."

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
        ZIG_EXTENSION=".tar.xz"
        ;;
    Darwin)
        ZIG_OS="macos"
        ZIG_EXTENSION=".tar.xz"
        ;;
    CYGWIN*)
        ZIG_OS="windows"
        ZIG_EXTENSION=".zip"
        ;;
    *)
        echo "Unknown OS"
        exit 1
        ;;
esac

ZIG_URL="${ZIG_MIRROR}/${ZIG_RELEASE}/zig-${ZIG_ARCH}-${ZIG_OS}-${ZIG_RELEASE}${ZIG_EXTENSION}"
ZIG_CHECKSUM_EXPECTED=$(echo "$ZIG_CHECKSUMS" | grep -F "$ZIG_URL" | cut -d ' ' -f 2)

# Work out the filename from the URL, as well as the directory without the ".tar.xz" file extension:
ZIG_ARCHIVE=$(basename "$ZIG_URL")
ZIG_DIRECTORY=$(basename "$ZIG_ARCHIVE" "$ZIG_EXTENSION")

# Download, making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
if command -v curl > /dev/null; then
    curl --silent --output "$ZIG_ARCHIVE" "$ZIG_URL"
elif command -v wget > /dev/null; then
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
    echo "Neither curl nor wget available."
    exit 1
fi

# Verify the checksum.
ZIG_CHECKSUM_ACTUAL=""
if command -v sha256sum > /dev/null; then
    ZIG_CHECKSUM_ACTUAL=$(sha256sum "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
elif command -v shasum > /dev/null; then
    ZIG_CHECKSUM_ACTUAL=$(shasum -a 256 "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
else
    echo "Neither sha256sum nor shasum available."
    exit 1
fi

if [ "$ZIG_CHECKSUM_ACTUAL" != "$ZIG_CHECKSUM_EXPECTED" ]; then
    echo "Checksum mismatch. Expected '$ZIG_CHECKSUM_EXPECTED' got '$ZIG_CHECKSUM_ACTUAL'."
    exit 1
fi

# Extract and then remove the downloaded archive:
echo "Extracting $ZIG_ARCHIVE..."
case "$ZIG_EXTENSION" in
    ".tar.xz")
        tar -xf "$ZIG_ARCHIVE"
        ;;
    ".zip")
        unzip -q "$ZIG_ARCHIVE"
        ;;
    *)
        echo "Unexpected error extracting Zig archive."
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
