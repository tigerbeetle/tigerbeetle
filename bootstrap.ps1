param(
    [switch]$build = $false
)

# Make sure we're in the repository root.
cd "$PSScriptRoot"

git fetch --tags --force --quiet

$version = $( git tag --sort=committerdate | Select-Object -Last 1 )

if ($build) {
    echo "Building TigerBeetle binary from source for your machine."
    echo ""

    git checkout $version
    .\scripts\install.bat
} else {
    echo "Downloading pre-built TigerBeetle binary for your machine."
    echo ""

    curl.exe -Lo tigerbeetle.zip "https://github.com/tigerbeetle/tigerbeetle/releases/download/$version/tigerbeetle-x86_64-windows.zip"
    unzip -qo tigerbeetle.zip
}

echo @"
Successfully set up $(./tigerbeetle version) at $(pwd)\tigerbeetle.exe.

To get started running TigerBeetle and interacting with it, see:

  https://github.com/tigerbeetle/tigerbeetle#running-tigerbeetle
"@
