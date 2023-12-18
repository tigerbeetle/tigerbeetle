@echo off
setlocal enabledelayedexpansion

REM Install Zig if it does not already exist:
if not exist "zig" (
    call .\scripts\install_zig.bat || exit /b
)

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
zig\zig.exe build install -Drelease -Dconfig=production || exit /b

for /l %%i in (0, 1, 0) do (
    echo Initializing replica %%i
    set ZIG_FILE=.\0_%%i.tigerbeetle.benchmark
    if exist "!ZIG_FILE!" DEL /F "!ZIG_FILE!"
    .\tigerbeetle.exe format --cluster=0 --replica=%%i --replica-count=1 !ZIG_FILE! > benchmark.log 2>&1 || exit /b
)

for /l %%i in (0, 1, 0) do (
    echo Starting replica %%i
    set ZIG_FILE=.\0_%%i.tigerbeetle.benchmark
    start /B "tigerbeetle_%%i" .\tigerbeetle.exe start --addresses=3001 !ZIG_FILE! > benchmark.log 2>&1
)

echo.
echo Benchmarking...

set ARGS=
for %%a in (%*) do (
    if not "%%a"==":main" (
        SET ARGS=!ARGS! %%a
    )
)
zig\zig.exe build benchmark -Drelease -Dconfig=production -- %ARGS%
exit /b %errorlevel%
