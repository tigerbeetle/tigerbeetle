@echo off
setlocal enabledelayedexpansion

git submodule init
git submodule update

cd .\tigerbeetle
.\scripts\install_zig.bat
echo "Building TigerBeetle..."
.\zig\zig.exe build -Dcpu=baseline -Drelease-safe
move .\zig-out\bin\tigerbeetle .
cd ..