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
cd .\src\zig\lib\tigerbeetle
.\zig\zig.exe build -Dcpu=baseline -Drelease-safe
move .\zig-out\bin\tigerbeetle.exe .
cd ..\..\..\..

echo "Building TigerBeetle Java Client"
cd .\src\tigerbeetle-java
mvn compile javac -sourcepath ./src -d ./build ./src/main/java/Benchmark.java ./src/main/java/com/tigerbeetle/*.java
cd ..\..

for /l %%i in (0, 1, 0) do (
    echo Initializing replica %%i
    set ZIG_FILE=.\0_%%i.tigerbeetle.benchmark
    if exist "!ZIG_FILE!" DEL /F "!ZIG_FILE!"
    .\src\zig\lib\tigerbeetle\tigerbeetle.exe format --cluster=0 --replica=%%i !ZIG_FILE! > benchmark.log 2>&1
)

for /l %%i in (0, 1, 0) do (
    echo Starting replica %%i
    set ZIG_FILE=.\0_%%i.tigerbeetle.benchmark
    start /B "tigerbeetle_%%i" .\src\zig\lib\tigerbeetle\tigerbeetle.exe start --addresses=3001 !ZIG_FILE! > benchmark.log 2>&1
)

rem Wait for replicas to start, listen and connect:
timeout /t 2

echo.
echo Benchmarking...
java -cp ./src/tigerbeetle-java/target/classes Benchmark
exit /b %errorlevel%
