@echo off
setlocal enabledelayedexpansion

git submodule init
git submodule update

cd ..\..\.. 
call .\scripts\install_zig.bat

echo "Building TigerBeetle..."
.\zig\zig.exe build -Dcpu=baseline -Drelease-safe
move .\zig-out\bin\tigerbeetle.exe .
cd src\clients\java

echo "Building TigerBeetle Java Client..."
cd .\src
mvn -B package --quiet
cd ..
