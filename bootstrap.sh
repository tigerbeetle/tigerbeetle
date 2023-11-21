#!/bin/sh

# Since this script potentially updates itself, putting the script in
# braces forces sh to parse the entire script before executing it so
# it isn't concurrently parsing a script that is changing on disk.
# https://stackoverflow.com/a/2358432/1507139
{
    from_source=""
    if [ "$1" = "-build" ]; then
        from_source="t"
    fi

    set -e

    # Make sure we're in the repository root.
    cd "$( dirname "$0" )"

    git fetch --tags --force --quiet
    version="$(git describe --tags "$(git rev-list --tags --max-count=1)")"
    os="$(uname)"
    arch="$(uname -m)"
    if [ "$from_source" = "" ]; then
        echo "Downloading pre-built TigerBeetle binary for your machine."
        echo ""

        if [ "$os" = "Darwin" ]; then
            arch="universal"
            os="macos"
        elif [ "$os" = "Linux" ]; then
            os="linux"
        else
            echo "Unsupported OS."
            exit 1
        fi

        curl -sLO "https://github.com/tigerbeetle/tigerbeetle/releases/download/$version/tigerbeetle-$arch-$os.zip"
        unzip -qo "tigerbeetle-$arch-$os.zip"
        chmod +x tigerbeetle
    else
        echo "Building TigerBeetle binary from source for your machine."
        echo ""

        git checkout "$version"
        ./scripts/install.sh
    fi

    echo "Successfully set up $(./tigerbeetle version) at $(pwd)/tigerbeetle.

To get started running TigerBeetle and interacting with it, see:

  https://github.com/tigerbeetle/tigerbeetle#running-tigerbeetle"

    # See https://stackoverflow.com/a/2358432/1507139.
    exit 0
}
