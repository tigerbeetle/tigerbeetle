#!/bin/bash
set -e

# Ask node for headers
HEADERS_URL=$(node -p 'process.release.headersUrl')

# Work out the filename from the URL, as well as the directory without the ".tar.gz" file extension:
rm -rf ./build
mkdir build
HEADERS_TARBALL=./build/`basename "$HEADERS_URL"`

# Download, making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
echo "Downloading $HEADERS_URL..."
if command -v wget &> /dev/null; then
    wget --quiet --show-progress --output-document=$HEADERS_TARBALL $HEADERS_URL
else
    curl --silent --progress-bar --output $HEADERS_TARBALL $HEADERS_URL
fi

# Extract and then remove the downloaded tarball:
echo "Extracting $HEADERS_TARBALL..."
tar -xf $HEADERS_TARBALL -C ./build
rm $HEADERS_TARBALL
