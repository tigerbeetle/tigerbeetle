@echo off
setlocal enabledelayedexpansion

if "%~1" equ ":main" (
    shift /1
    goto main
)

cmd /d /c "%~f0" :main %*
set ZIG_RESULT=%ERRORLEVEL%
taskkill /F /IM tigerbeetle.exe >nul

if !ZIG_RESULT! equ 0 (
    del /f benchmark.log
) else (
    echo.
    echo Error running benchmark, here are more details
    type benchmark.log
)

echo.
exit /b

:main

echo "Building TigerBeetle..."
cd ..\..\..
.\zig\zig.exe build install -Drelease
cd src/clients/dotnet

for /l %%i in (0, 1, 0) do (
    echo Initializing replica %%i
    set ZIG_FILE=.\0_%%i.tigerbeetle.benchmark
    if exist "!ZIG_FILE!" DEL /F "!ZIG_FILE!"
    ..\..\..\tigerbeetle.exe format --cluster=0 --replica=%%i --replica-count=1 !ZIG_FILE! > benchmark.log 2>&1
)

for /l %%i in (0, 1, 0) do (
    echo Starting replica %%i
    set ZIG_FILE=.\0_%%i.tigerbeetle.benchmark
    start /B "tigerbeetle_%%i" ..\..\..\tigerbeetle.exe start --addresses=3001 !ZIG_FILE! > benchmark.log 2>&1
)

rem Wait for replicas to start, listen and connect:
timeout /t 2

echo.
echo Benchmarking...
cd .\TigerBeetle.Benchmarks
dotnet run -c Release
cd ..\..
exit /b %errorlevel%
