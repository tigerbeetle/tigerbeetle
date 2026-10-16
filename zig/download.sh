#!/usr/bin/env sh
set -eu

ZIG_MIRROR="https://ziglang.org/download"
ZIG_RELEASE="0.16.0"
ZIG_CHECKSUMS=$(cat<<EOF
${ZIG_MIRROR}/0.16.0/zig-aarch64-linux-0.16.0.tar.xz ea4b09bfb22ec6f6c6ceac57ab63efb6b46e17ab08d21f69f3a48b38e1534f17
${ZIG_MIRROR}/0.16.0/zig-aarch64-macos-0.16.0.tar.xz b23d70deaa879b5c2d486ed3316f7eaa53e84acf6fc9cc747de152450d401489
${ZIG_MIRROR}/0.16.0/zig-aarch64-windows-0.16.0.zip aee38316ee4111717900f45dd3130145c39289e105541d737eb8c5ed653c78ef
${ZIG_MIRROR}/0.16.0/zig-x86_64-linux-0.16.0.tar.xz 70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00
${ZIG_MIRROR}/0.16.0/zig-x86_64-macos-0.16.0.tar.xz 0387557ed1877bc6a2e1802c8391953baddba76081876301c522f52977b52ba7
${ZIG_MIRROR}/0.16.0/zig-x86_64-windows-0.16.0.zip 68659eb5f1e4eb1437a722f1dd889c5a322c9954607f5edcf337bc3684a75a7e
EOF
)

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
ZIG_ARCHIVE="./zig/cache/$(basename "$ZIG_URL")"
ZIG_DIRECTORY=$(basename "$ZIG_ARCHIVE" "$ZIG_EXTENSION")

# Returns 0 if the given file exists and its SHA-256 checksum matches the expected value.
checksum_valid() {
    [ -f "$ZIG_ARCHIVE" ] || return 1
    ZIG_CHECKSUM_ACTUAL=""
    if command -v sha256sum > /dev/null; then
        ZIG_CHECKSUM_ACTUAL=$(sha256sum "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
    elif command -v shasum > /dev/null; then
        ZIG_CHECKSUM_ACTUAL=$(shasum -a 256 "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
    else
        echo "Neither sha256sum nor shasum available."
        exit 1
    fi
    [ "$ZIG_CHECKSUM_ACTUAL" = "$ZIG_CHECKSUM_EXPECTED" ]
}

if checksum_valid; then # Caching for CI.
    echo "Skip downloading Zig $ZIG_RELEASE."
else
    echo "Downloading Zig $ZIG_RELEASE ..."
    mkdir -p ./zig/cache
    # Download, making sure we download to the same output document, without
    # wget adding "-1" etc. if the file was previously partially downloaded:
    if command -v curl > /dev/null; then
        curl --location --silent --show-error --output "$ZIG_ARCHIVE" "$ZIG_URL"
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
    if ! checksum_valid; then
        echo "Checksum mismatch."
        exit 1
    fi
fi

echo "Extracting $ZIG_ARCHIVE ..."
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
# NB: Keep archive for caching.

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
