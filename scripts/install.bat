@echo off
setlocal enabledelayedexpansion

:: Install Zig if it does not already exist:
if not exist zig\ (
  call .\scripts\install_zig.bat
)

echo "Building TigerBeetle..."
.\zig\zig.exe build install -Drelease
