@echo off
setlocal enabledelayedexpansion

echo "Building TigerBeetle..."
cd ..\..\..
.\zig\zig.exe build install -Dcpu=baseline -Drelease-safe
cd src\clients\java

set ZIG_FILE=.\0_0.tigerbeetle.examples

echo Initializing replica
if exist "!ZIG_FILE!" DEL /F "!ZIG_FILE!"
..\..\..tigerbeetle.exe format --cluster=0 --replica=0 !ZIG_FILE!

echo Starting replica
..\..\..tigerbeetle.exe start --addresses=3000 !ZIG_FILE!
