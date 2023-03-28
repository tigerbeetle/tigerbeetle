@echo off
setlocal enabledelayedexpansion

call .\scripts\install_zig.bat

echo "Building TigerBeetle..."
.\zig\zig.exe build install -Dcpu=baseline -Drelease-safe
