@echo off
setlocal enabledelayedexpansion

zig\zig.exe build -Drelease-safe
move zig-out\bin\tigerbeetle.exe . >nul

for /l %%i in (0, 1, 0) do (
    echo Initializing replica %%i
    set ZIG_FILE=cluster_0000000000_replica_00%%i.tigerbeetle
    if exist "!ZIG_FILE!" DEL "!ZIG_FILE"! /s
    .\tigerbeetle.exe init --directory=. --cluster=0 --replica=%%i > benchmark.log 2>&1
)