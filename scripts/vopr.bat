:: Installs Zig if needed and runs the VOPR
@echo off

:: Install Zig if a zig folder does not already exist:
if not exist zig\ (
    :: Installs the latest version of Zig
    call scripts\install_zig.bat
    :: Checks that the Zig folder now exists
    if not exist zig\ (
        echo The Zig installation failed.
        exit 1
    )
    echo Running the TigerBeetle VOPR for the first time...
    echo Visit https://www.tigerbeetle.com
)

:: If a seed is provided as an argument then replay the seed, otherwise test 1,000 seeds:
if not "%~1"=="" (
    :: Build in fast ReleaseSafe mode if required, useful where you don't need debug logging:
    if "%~2"=="-OReleaseSafe" (
        echo Replaying seed %~1 in ReleaseSafe mode...
        call zig\zig run src\simulator.zig -OReleaseSafe -- %~1
        if not %ERRORLEVEL%==0 (
            echo Cannot replay the %~1 seed using the VOPR.
            exit 1
        )
    ) else (
        echo Replaying seed %~1 in Debug mode with full debug logging enabled...
        call zig\zig run src\simulator.zig -ODebug -- %~1
        if not %ERRORLEVEL%==0 (
            echo Cannot run the VOPR.
            exit 1
        )
    )
) else (
    call zig\zig build-exe src\simulator.zig -OReleaseSafe
        if not %ERRORLEVEL%==0 (
            echo Cannot run the VOPR.
            exit 1
        )
    for %%i in (1,1,1000) do (
        call simulator
        if not %ERRORLEVEL%==0 (
            echo Cannot run a seed using the VOPR.
            exit 1
        )
    )
)