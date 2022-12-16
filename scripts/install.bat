@echo off
setlocal enabledelayedexpansion

call .\scripts\install_zig.bat

echo "Building TigerBeetle..."
.\zig\zig.exe build -Dcpu=baseline -Drelease-safe
