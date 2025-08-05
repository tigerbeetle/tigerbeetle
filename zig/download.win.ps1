# Windows-only PowerShell script to download and install Zig
$ErrorActionPreference = "Stop"

$ZIG_RELEASE = "0.14.1"
$ZIG_ARCH = "x86_64"
$ZIG_OS = "windows"
$ZIG_EXTENSION = ".zip"

# List of expected checksums
$ZIG_CHECKSUMS = @"
https://ziglang.org/download/0.14.1/zig-aarch64-windows-0.14.1.zip b5aac0ccc40dd91e8311b1f257717d8e3903b5fefb8f659de6d65a840ad1d0e7
https://ziglang.org/download/0.14.1/zig-x86_64-windows-0.14.1.zip 554f5378228923ffd558eac35e21af020c73789d87afeabf4bfd16f2e6feed2c
"@

# Build URL
$ZIG_URL = "https://ziglang.org/download/$ZIG_RELEASE/zig-$ZIG_ARCH-$ZIG_OS-$ZIG_RELEASE$ZIG_EXTENSION"
$ZIG_ARCHIVE = [System.IO.Path]::GetFileName($ZIG_URL)
$ZIG_DIRECTORY = $ZIG_ARCHIVE -replace [regex]::Escape($ZIG_EXTENSION), ""

# Find expected checksum from list
$ZIG_CHECKSUM_EXPECTED = ($ZIG_CHECKSUMS -split "`n" | Where-Object { $_ -like "*$ZIG_URL*" }) -split ' ' | Select-Object -Last 1

Write-Host "Downloading Zig $ZIG_RELEASE for Windows..."
Invoke-WebRequest -Uri $ZIG_URL -OutFile $ZIG_ARCHIVE

# Compute SHA256 checksum using .NET
$sha256 = [System.Security.Cryptography.SHA256]::Create()
$stream = [System.IO.File]::OpenRead($ZIG_ARCHIVE)
$hash = $sha256.ComputeHash($stream)
$stream.Close()
$ZIG_CHECKSUM_ACTUAL = ($hash | ForEach-Object { $_.ToString("x2") }) -join ""

if ($ZIG_CHECKSUM_ACTUAL -ne $ZIG_CHECKSUM_EXPECTED) {
    Write-Error "Checksum mismatch. Expected '$ZIG_CHECKSUM_EXPECTED' but got '$ZIG_CHECKSUM_ACTUAL'."
    exit 1
}

Write-Host "Extracting $ZIG_ARCHIVE..."
Expand-Archive -Path $ZIG_ARCHIVE -DestinationPath .

Remove-Item $ZIG_ARCHIVE

# Create output directory if not exists
if (-Not (Test-Path zig)) {
    New-Item -ItemType Directory -Path zig | Out-Null
}

# Clean old contents
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue zig/doc, zig/lib

# Move relevant files
Move-Item "$ZIG_DIRECTORY/LICENSE" zig/
Move-Item "$ZIG_DIRECTORY/README.md" zig/
Move-Item "$ZIG_DIRECTORY/doc" zig/
Move-Item "$ZIG_DIRECTORY/lib" zig/
Move-Item "$ZIG_DIRECTORY/zig.exe" zig/

# Cleanup extracted directory
Remove-Item "$ZIG_DIRECTORY"

$ZIG_BIN = Join-Path (Get-Location) "zig\zig.exe"
Write-Host "Downloading completed ($ZIG_BIN)! Enjoy!"
