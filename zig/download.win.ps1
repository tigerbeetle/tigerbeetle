$ErrorActionPreference = "Stop"

$ZIG_MIRROR="https://pkg.machengine.org/zig"
$ZIG_RELEASE = "0.14.1"
$ZIG_CHECKSUMS = @"
$ZIG_MIRROR/0.14.1/zig-aarch64-windows-0.14.1.zip b5aac0ccc40dd91e8311b1f257717d8e3903b5fefb8f659de6d65a840ad1d0e7
$ZIG_MIRROR/0.14.1/zig-x86_64-windows-0.14.1.zip 554f5378228923ffd558eac35e21af020c73789d87afeabf4bfd16f2e6feed2c
"@

$ZIG_ARCH = if ($env:PROCESSOR_ARCHITECTURE -eq "ARM64") {
    "aarch64"
} elseif ($env:PROCESSOR_ARCHITECTURE -eq "AMD64") {
    "x86_64"
} else {
    Write-Error "Unsupported architecture: $($env:PROCESSOR_ARCHITECTURE)"
    exit 1
}
$ZIG_OS = "windows"
$ZIG_EXTENSION = ".zip"

# Build URL:
$ZIG_URL = "$ZIG_MIRROR/$ZIG_RELEASE/zig-$ZIG_ARCH-$ZIG_OS-$ZIG_RELEASE$ZIG_EXTENSION"
$ZIG_ARCHIVE = [System.IO.Path]::GetFileName("$ZIG_URL")
$ZIG_DIRECTORY = "$ZIG_ARCHIVE" -replace [regex]::Escape($ZIG_EXTENSION), ""

# Find expected checksum from list:
$ZIG_CHECKSUM_EXPECTED = ($ZIG_CHECKSUMS -split "`n" | Where-Object { $_ -like "*$ZIG_URL*" }) -split ' ' | Select-Object -Last 1

Write-Output "Downloading Zig $ZIG_RELEASE for Windows..."
Invoke-WebRequest -Uri "$ZIG_URL" -OutFile "$ZIG_ARCHIVE"

# Verify the checksum.
$ZIG_CHECKSUM_ACTUAL=(Get-FileHash "${ZIG_ARCHIVE}").Hash

if ($ZIG_CHECKSUM_ACTUAL -ne $ZIG_CHECKSUM_EXPECTED) {
    Write-Error "Checksum mismatch. Expected '$ZIG_CHECKSUM_EXPECTED' but got '$ZIG_CHECKSUM_ACTUAL'."
    exit 1
}

# Extract and then remove the downloaded archive:
Write-Output "Extracting $ZIG_ARCHIVE..."
Expand-Archive -Path "$ZIG_ARCHIVE" -DestinationPath .
Remove-Item "$ZIG_ARCHIVE"

# Replace these existing directories and files so that we can install or upgrade:
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue zig/doc, zig/lib
Move-Item "$ZIG_DIRECTORY/LICENSE" zig/
Move-Item "$ZIG_DIRECTORY/README.md" zig/
Move-Item "$ZIG_DIRECTORY/doc" zig/
Move-Item "$ZIG_DIRECTORY/lib" zig/
Move-Item "$ZIG_DIRECTORY/zig.exe" zig/

# We expect to have now moved all directories and files out of the extracted directory.
# Do not force remove so that we can get an error if the above list of files ever changes:
Remove-Item "$ZIG_DIRECTORY"

# It's up to the user to add this to their path if they want to:
$ZIG_BIN = Join-Path (Get-Location) "zig\zig.exe"
Write-Output "Downloading completed ($ZIG_BIN)! Enjoy!"
