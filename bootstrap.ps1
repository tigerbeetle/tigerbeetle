param(
    [switch]$build = $false
)

# Make sure we're in the repository root.
cd "$PSScriptRoot"

git fetch --tags --force --quiet

$version = $( git tag --sort=committerdate | Select-Object -Last 1 )

if ($build) {
    git checkout $version
    .\scripts\install.bat
} else {
    curl -o tigerbeetle.zip "https://github.com/tigerbeetle/tigerbeetle/releases/download/$version/tigerbeetle-x86_64-windows-$version.zip"
    unzip -qo tigerbeetle.zip
}

echo "Installed at $(pwd)\tigerbeetle.exe: $(.\tigerbeetle.exe version)."
