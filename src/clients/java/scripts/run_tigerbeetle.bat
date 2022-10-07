@echo off
setlocal enabledelayedexpansion

echo "Building TigerBeetle..."
cd .\src\zig\lib\tigerbeetle
.\zig\zig.exe build -Dcpu=baseline -Drelease-safe
move .\zig-out\bin\tigerbeetle.exe .
cd ..\..\..\..

set ZIG_FILE=.\0_0.tigerbeetle.samples

echo Initializing replica
if exist "!ZIG_FILE!" DEL /F "!ZIG_FILE!"
.\src\zig\lib\tigerbeetle\tigerbeetle.exe format --cluster=0 --replica=0 !ZIG_FILE!

echo Starting replica
.\src\zig\lib\tigerbeetle\tigerbeetle.exe start --addresses=3001 !ZIG_FILE!
