@echo off

CALL :absolute_path ".\tigerbeetle\zig\zig.exe"
SET ZIG_EXE=%RETVAL%

echo @echo off> zigcc.bat
echo %ZIG_EXE% cc %%*>> zigcc.bat

CALL :absolute_path ".\zigcc.bat"
set ZIG_CC=%RETVAL%

SET CC=%ZIG_CC%
SET CGO_ENABLED=1
go %*
EXIT /B

:absolute_path
    set RETVAL=%~f1
    EXIT /B