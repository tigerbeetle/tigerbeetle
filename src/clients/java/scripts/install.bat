@echo off
setlocal enabledelayedexpansion

git submodule init
git submodule update

cd .\src\zig\lib\tigerbeetle
call .\scripts\install_zig.bat

echo "Building TigerBeetle..."
.\zig\zig.exe build -Dcpu=baseline -Drelease-safe
move .\zig-out\bin\tigerbeetle.exe .
cd ..\..\..\..

echo "Building TigerBeetle Java Client..."
cd .\src\tigerbeetle-java
mvn -B clean package --quiet
cd ..\..
