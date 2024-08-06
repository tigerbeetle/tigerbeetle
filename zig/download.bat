@echo off

set ZIG_RELEASE_DEFAULT=0.13.0

:: Determine the Zig build:
if "%~1"=="" (
    set ZIG_RELEASE=%ZIG_RELEASE_DEFAULT%
) else if "%~1"=="latest" (
    set ZIG_RELEASE=builds
) else (
    set ZIG_RELEASE=%~1
)

:: Checks format of release version.
echo.%ZIG_RELEASE% | findstr /b /r /c:"builds" /c:"^[0-9][0-9]*.[0-9][0-9]*.[0-9][0-9]*">nul || (echo.Unexpected release format. && exit 1)

set ZIG_OS=windows
set ZIG_ARCH=x86_64

set ZIG_TARGET=zig-%ZIG_OS%-%ZIG_ARCH%

:: Determine the build, split the JSON line on whitespace and extract the 2nd field:
for /f "tokens=2" %%a in ('curl --silent https://ziglang.org/download/index.json ^| findstr %ZIG_TARGET% ^| findstr %ZIG_RELEASE%' ) do (
  set ZIG_URL=%%a
)

:: Then remove quotes and commas:
for /f %%b in ("%ZIG_URL:,=%") do (
    set ZIG_URL=%%~b
)

:: Checks the ZIG_URL variable follows the expected format.
echo.%ZIG_URL% | findstr /b /r /c:"https://ziglang.org/builds/" /c:"https://ziglang.org/download/%ZIG_RELEASE%">nul || (echo.Unexpected release URL format. && exit 1)

if "%ZIG_RELEASE%"=="builds" (
    echo Downloading Zig latest build...
) else (
    echo Downloading Zig %ZIG_RELEASE% release build...
)

:: Using variable modifiers to determine the directory and filename from the URL:
:: %%~ni Expands %%i to a file name only and %%~xi Expands %%i to a file name extension only.
for /f %%i in ("%ZIG_URL%") do (
    set ZIG_DIRECTORY=%%~ni
    set ZIG_TARBALL=%%~nxi
)

:: Checks the ZIG_DIRECTORY variable follows the expected format.
echo.%ZIG_DIRECTORY% | findstr /b /r /c:"zig-win64-" /c:"zig-windows-x86_64-">nul || (echo.Unexpected zip directory name format. && exit 1)

:: Making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
if exist %ZIG_TARBALL% (
  del /q %ZIG_TARBALL%
  if exist %ZIG_TARBALL% (
    echo Failed to delete %ZIG_TARBALL%.
    exit 1
  )
)

echo Downloading %ZIG_URL%...
curl --silent --progress-bar --output %ZIG_TARBALL% %ZIG_URL%
if not exist  %ZIG_TARBALL% (
  echo Failed to download zip file.
  exit 1
)

:: Extract and then remove the downloaded tarball:
:: Hiding Powershell's progress bar during the extraction
SET PS_DISABLE_PROGRESS="$ProgressPreference=[System.Management.Automation.ActionPreference]::SilentlyContinue"
powershell -Command "%PS_DISABLE_PROGRESS%;Expand-Archive %ZIG_TARBALL% -DestinationPath ."

if not exist %ZIG_TARBALL% (
  echo Failed to extract zip file.
  exit 1
)

if exist zig\doc (
  rd /s /q zig\doc
  if exist zig\doc (
    echo The zig\doc directory could not be deleted.
    exit 1
  )
)

if exist zig\lib (
  rd /s /q zig\lib
  if exist zig\lib (
    echo The zig\lib directory could not be deleted.
    exit 1
  )
)

move /Y %ZIG_DIRECTORY%\LICENSE zig\LICENSE>nul
move /Y %ZIG_DIRECTORY%\README.md zig\README.md>nul
move /Y %ZIG_DIRECTORY%\doc zig>nul
move /Y %ZIG_DIRECTORY%\lib zig>nul
move /Y %ZIG_DIRECTORY%\zig.exe zig\zig.exe>nul

rd /s /q %ZIG_DIRECTORY%
if exist %ZIG_DIRECTORY% (
  echo The %ZIG_DIRECTORY% directory could not be deleted.
  exit 1
)

del /q %ZIG_TARBALL%
if exist %ZIG_TARBALL% (
  echo The %ZIG_TARBALL% file could not be deleted.
  exit 1
)

echo "Downloading completed (%ZIG_DIRECTORY%\zig.exe)! Enjoy!"