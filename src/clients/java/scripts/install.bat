@echo off
setlocal enabledelayedexpansion

git submodule init
git submodule update

cd .\src\zig\lib\tigerbeetle
.\scripts\install_zig.bat
echo "Building TigerBeetle..."
.\zig\zig.exe build -Dcpu=baseline -Drelease-safe
mv .\zig-out\bin\tigerbeetle .
cd ..\..\..\..